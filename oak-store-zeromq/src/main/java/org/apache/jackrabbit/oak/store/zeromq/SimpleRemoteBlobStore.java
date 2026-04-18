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

    /** Total number of remote hasBlob() calls made. */
    private final AtomicLong hasBlobCalls = new AtomicLong();
    /** Calls where the local cache already had the blob (remote call was redundant). */
    private final AtomicLong hasBlobRedundant = new AtomicLong();
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
        long t0 = System.nanoTime();
        writer.accept(ref, localCache.getInputStream(ref));
        log.info("putTempBlob({}) remote write in {}ms", ref, (System.nanoTime() - t0) / 1_000_000);
        return ref;
    }

    @Override
    public void putTempBlobAs(String ref, TemporaryBlob tempBlob) throws IOException {
        checkEmergency();
        localCache.putTempBlobAs(ref, tempBlob);
        if (ref.contains("journal") || !checker.apply(ref)) {
            long t0 = System.nanoTime();
            writer.accept(ref, localCache.getInputStream(ref));
            log.info("putTempBlobAs({}) remote write in {}ms", ref, (System.nanoTime() - t0) / 1_000_000);
        }
    }

    @Override
    public boolean hasBlob(String ref) {
        checkEmergency();
        long total = hasBlobCalls.incrementAndGet();
        boolean inCache = localCache.hasBlob(ref);
        if (inCache) {
            hasBlobRedundant.incrementAndGet();
        }
        long t0 = System.nanoTime();
        boolean result = checker.apply(ref);
        long ms = (System.nanoTime() - t0) / 1_000_000;
        if (total % LOG_INTERVAL == 0) {
            log.info("hasBlob stats: {} total remote checks, {} redundant (local cache hit), last check {}ms",
                    total, hasBlobRedundant.get(), ms);
        } else {
            log.debug("hasBlob({}) inCache={} remote={} in {}ms", ref, inCache, result, ms);
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
