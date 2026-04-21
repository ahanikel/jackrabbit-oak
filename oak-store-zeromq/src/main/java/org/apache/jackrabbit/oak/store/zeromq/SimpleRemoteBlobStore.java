/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.store.zeromq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class SimpleRemoteBlobStore implements BlobStore {

    private static final Logger log = LoggerFactory.getLogger(SimpleRemoteBlobStore.class);
    private static final int WORKER_THREADS = 50;
    private final Function<String, Boolean> checker;
    private final Function<String, InputStream> reader;
    private final BiConsumer<String, InputStream> writer;
    private final BlobStore localCache;
    private final ExecutorService threads;
    private volatile boolean emergency = false;
    private final List<Future<?>> pendingWrites = new ArrayList<>();

    /** hasBlob() calls answered from local cache (no Azure round-trip). */
    private final AtomicLong hasBlobCacheHits = new AtomicLong();
    /** hasBlob() calls that required an Azure round-trip (blob not in local cache). */
    private final AtomicLong hasBlobRemoteCalls = new AtomicLong();
    private static final long LOG_INTERVAL = 100;

    public SimpleRemoteBlobStore(Function<String, Boolean> checker, Function<String, InputStream> reader,
                                 BiConsumer<String, InputStream> writer, BlobStore localCache) {
        this.checker = checker;
        this.reader = reader;
        this.writer = writer;
        this.localCache = localCache;
        threads = Executors.newFixedThreadPool(WORKER_THREADS, new NamedThreadFactory("SimpleRemoteBlobStore"));
    }

    private void ensureBlobInCache(String ref) throws IOException {
        checkEmergency();
        if (ref.contains("journal") || !localCache.hasBlob(ref)) {
          InputStream is = reader.apply(ref);
          if (is == null) {
              throw new FileNotFoundException("Blob not found: " + ref);
          }
          localCache.putInputStreamAs(ref, is);
        }
    }

    @Override
    public byte[] getBytes(String ref) throws IOException {
        ensureBlobInCache(ref);
        return localCache.getBytes(ref);
    }

    @Override
    public String getString(String ref) throws IOException {
        ensureBlobInCache(ref);
        return localCache.getString(ref);
    }

    @Override
    public InputStream getInputStream(String ref) throws IOException {
        ensureBlobInCache(ref);
        return localCache.getInputStream(ref);
    }

    @Override
    public String putBytes(byte[] bytes) throws IOException, BlobAlreadyExistsException {
        checkEmergency();
        final String ref = localCache.putBytes(bytes);
        if (!checker.apply(ref)) {
            writer.accept(ref, new ByteArrayInputStream(bytes));
        }
        return ref;
    }

    @Override
    public String putInputStream(InputStream is) throws IOException, BlobAlreadyExistsException {
        checkEmergency();
        final String ref = localCache.putInputStream(is);
        submit(() -> {
            boolean emergencySet = false;
            while (true) {
                try {
                    if (!checker.apply(ref)) {
                        writer.accept(ref, localCache.getInputStream(ref));
                    }
                    if (emergencySet) {
                        emergency = false;
                    }
                    break;
                } catch (IOException e) {
                    emergency = true;
                    emergencySet = true;
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException ie) {
                        break;
                    }
                }
            }
        });
        return ref;
    }

    @Override
    public void putInputStreamAs(String ref, InputStream is) throws IOException {
        checkEmergency();
        localCache.putInputStreamAs(ref, is);
        if (ref.contains("journal") || !checker.apply(ref)) {
            long t0 = System.nanoTime();
            writer.accept(ref, localCache.getInputStream(ref));
            log.info("putInputStreamAs({}) remote write in {}ms", ref, (System.nanoTime() - t0) / 1_000_000);
        }
    }

    @Override
    public TemporaryBlob getTempBlob() throws IOException {
        checkEmergency();
        return localCache.getTempBlob();
    }

    @Override
    public String putTempBlob(TemporaryBlob tempFile) throws BlobAlreadyExistsException, IOException {
        checkEmergency();
        final String ref = localCache.putTempBlob(tempFile);
        // Remote write is async — the journal barrier (flushPendingWrites) ensures
        // it completes before the journal is committed.
        submit(() -> {
            try {
                if (!checker.apply(ref)) {
                    long t0 = System.nanoTime();
                    writer.accept(ref, localCache.getInputStream(ref));
                    log.info("putTempBlob({}) async remote write in {}ms", ref, (System.nanoTime() - t0) / 1_000_000);
                }
            } catch (IOException e) {
                log.error("putTempBlob({}) async remote write failed: {}", ref, e.getMessage());
                emergency = true;
            }
        });
        return ref;
    }

    @Override
    public void putTempBlobAs(String ref, TemporaryBlob tempBlob) throws IOException {
        checkEmergency();
        localCache.putTempBlobAs(ref, tempBlob);
        if (ref.contains("journal")) {
            // Journal must be durably written synchronously — other nodes read it immediately.
            long t0 = System.nanoTime();
            writer.accept(ref, localCache.getInputStream(ref));
            log.info("putTempBlobAs({}) journal write in {}ms", ref, (System.nanoTime() - t0) / 1_000_000);
        } else {
            // Content-addressed segment blobs: async remote write; journal barrier waits for them.
            submit(() -> {
                try {
                    if (!checker.apply(ref)) {
                        long t0 = System.nanoTime();
                        writer.accept(ref, localCache.getInputStream(ref));
                        log.info("putTempBlobAs({}) async remote write in {}ms", ref, (System.nanoTime() - t0) / 1_000_000);
                    }
                } catch (IOException e) {
                    log.error("putTempBlobAs({}) async remote write failed: {}", ref, e.getMessage());
                    emergency = true;
                }
            });
        }
    }

    @Override
    public void flushPendingWrites() throws IOException {
        synchronized (pendingWrites) {
            int count = pendingWrites.size();
            long t0 = System.nanoTime();
            for (Future<?> f : pendingWrites) {
                try {
                    f.get();
                } catch (Exception e) {
                    log.error("Async remote write failed during flush: {}", e.getMessage());
                }
            }
            pendingWrites.clear();
            if (count > 0) {
                log.info("flushPendingWrites: waited for {} async remote writes in {}ms",
                        count, (System.nanoTime() - t0) / 1_000_000);
            }
        }
    }

    @Override
    public boolean hasBlob(String ref) {
        checkEmergency();
        // Fast path: if the blob is in the local cache it was successfully written to the
        // remote store in a prior call (content-addressed; we never evict). Skip the
        // expensive Azure exists() round-trip (~110 ms each).
        if (localCache.hasBlob(ref)) {
            long hits = hasBlobCacheHits.incrementAndGet();
            if (hits % LOG_INTERVAL == 0) {
                log.info("hasBlob stats: {} cache hits, {} remote checks",
                        hits, hasBlobRemoteCalls.get());
            } else {
                log.debug("hasBlob({}) cache hit", ref);
            }
            return true;
        }
        long t0 = System.nanoTime();
        boolean result = checker.apply(ref);
        long ms = (System.nanoTime() - t0) / 1_000_000;
        long remote = hasBlobRemoteCalls.incrementAndGet();
        if (remote % LOG_INTERVAL == 0) {
            log.info("hasBlob stats: {} cache hits, {} remote checks, last remote {}ms",
                    hasBlobCacheHits.get(), remote, ms);
        } else {
            log.debug("hasBlob({}) remote={} in {}ms", ref, result, ms);
        }
        return result;
    }

    @Override
    public long getLength(String ref) throws IOException {
        ensureBlobInCache(ref);
        return localCache.getLength(ref);
    }

    private void checkEmergency() {
        while (emergency) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private void submit(Runnable r) {
        synchronized (pendingWrites) {
            pendingWrites.removeIf(Future::isDone);
            if (pendingWrites.size() > WORKER_THREADS) {
                Future<?> f = pendingWrites.remove(0);
                try {
                    f.get();
                } catch (Exception e) {
                    // ignore
                }
            }
            Future<?> f = threads.submit(r);
            pendingWrites.add(f);
        }
    }
}
