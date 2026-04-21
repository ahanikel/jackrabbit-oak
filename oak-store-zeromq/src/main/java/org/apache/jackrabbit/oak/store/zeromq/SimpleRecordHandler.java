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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SimpleRecordHandler {

    private static final Logger log = LoggerFactory.getLogger(SimpleRecordHandler.class);
    private static final String JOURNAL_TOPIC = SimpleRequestResponse.Topic.JOURNAL.toString();

    private static class CurrentBlob {
        private String ref;
        private TemporaryBlob temporaryBlob;
        private InputStream found;

        public String getRef() {
            return ref;
        }

        public CurrentBlob setRef(String ref) {
            this.ref = ref;
            return this;
        }

        public TemporaryBlob getTemporaryBlob() {
            return temporaryBlob;
        }

        public CurrentBlob setTemporaryBlob(TemporaryBlob temporaryBlob) {
            this.temporaryBlob = temporaryBlob;
            return this;
        }

        public InputStream getFound() {
            return found;
        }

        public CurrentBlob setFound(InputStream found) {
            this.found = found;
            return this;
        }
    }

    private final Base64.Decoder b64 = Base64.getDecoder();
    private int line = 0;
    private final BlobStore store;
    private final Map<String, CurrentBlob> currentBlobMap;
    private final Cache<String, Long> lastMessageSeen;
    private final ZMQ.Socket journalPublisher;
    private final ExecutorService threads;
    private final List<Future<?>> pendingTasks;

    public SimpleRecordHandler(BlobStore store, ZMQ.Socket journalPublisher) {
        this.store = store;
        currentBlobMap = new ConcurrentHashMap<>();
        lastMessageSeen = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();
        this.journalPublisher = journalPublisher;
        this.threads = Executors.newFixedThreadPool(100);
        pendingTasks = Collections.synchronizedList(new ArrayList<>());
    }

    public void handleRecord(String uuThreadId, long msgid, String op, byte[] value) throws IOException {

        synchronized (this) {
            ++line;
            if (line % 100000 == 0) {
                log.info("We're at line {}", line);
            }

            Long lastMsgId = lastMessageSeen.getIfPresent(uuThreadId);
            if (lastMsgId != null && lastMsgId.longValue() >= msgid) {
                log.info("Duplicate msgId: {} instead of {}", msgid, lastMsgId + 1);
                return;
            }
            lastMessageSeen.put(uuThreadId, msgid);
        }

        if (op == null) {
            return;
        }

        boolean raw = false;

        // Message format: uuThreadId msgId op value
        //                                  n: newUuid oldUuid   (begin new node)
        //                                  n!                   (end (store) new node)
        //                                  n+ name uuid         (add    child node)
        //                                  n^ name uuid         (change child node)
        //                                  n- name              (delete child node)
        //                                  p+ name value        (add    property,
        //                                    where value ::= '<' type '>' (simpleValue | ('[' [simpleValue {',' simpleValue}] ']')
        //                                    where simpleValue is a safeEncoded (kind of urlencoded) string
        //                                    and type is always the singular type, even in case of a list of values
        //                                  p^ name value        (change property)
        //                                  p- name              (remove property)
        //                                b64+ uuid              (begin new blob)
        //                                b64x                   (cancel current blob)
        //                                braw data              (a chunk of unencoded (raw) binary data)
        //                                b64d data              (a chunk of base64-encoded binary data)
        //                                b64!                   (end new blob)
        //                                journal journal-id newHead oldHead (set the new journal head)
        //                                                       the journal forms a barrier in the sense that all
        //                                                       blobs which came before must have been written to
        //                                                       the blob store.
        //                                                       the journal files are the only blobs that are not
        //                                                       named after their uuid. they must contain the string
        //                                                       "journal" in their name. the blobstore recognises this
        //                                                       and waits for all pending blob uploads to finish.
        switch (op) {
            case "b64+": {
                StringTokenizer tokens = new StringTokenizer(new String(value));
                final String ref = tokens.nextToken();
                CurrentBlob currentBlob = currentBlobMap.get(uuThreadId);
                if (currentBlob == null) {
                    currentBlob = new CurrentBlob();
                    currentBlob.setTemporaryBlob(store.getTempBlob());
                    currentBlobMap.put(uuThreadId, currentBlob);
                }
                final String currentBlobRef = currentBlob.getRef();
                if (currentBlobRef != null) {
                    final String msg = "Blob " + currentBlobRef + " still open when starting with new blob " + ref;
                    log.error(msg);
                    throw new IllegalStateException(msg);
                }
                if (currentBlob.getFound() != null) {
                    final String msg = "currentBlobFound is not null";
                    log.error(msg);
                    throw new IllegalStateException(msg);
                }
                    currentBlob.setRef(ref);
                    for (int i = 0; ; ++i) {
                        try {
                            TemporaryBlob tempBlob = store.getTempBlob();
                            currentBlob.setTemporaryBlob(tempBlob);
                            break;
                        } catch (IOException ioe) {
                            if (i % 600 == 0) {
                                log.error("Unable to create temp file, retrying every 100ms (#{}): {}", i, ioe.getMessage());
                            }
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException interruptedException) {
                                log.info(interruptedException.getMessage());
                                break;
                            }
                        }
                    }
                break;
            }

            case "b64x": {
                final CurrentBlob currentBlob = currentBlobMap.get(uuThreadId);
                if (currentBlob.getFound() != null) {
                    currentBlob.setFound(null);
                }
                final TemporaryBlob temporaryBlob = currentBlob.getTemporaryBlob();
                if (temporaryBlob != null) {
                    try {
                        temporaryBlob.delete();
                    } catch (IOException e) {
                        log.warn(e.getMessage());
                    }
                    currentBlob.setTemporaryBlob(null);
                }
                currentBlob.setRef(null);
                break;
            }

            case "braw":
                raw = true;
            case "b64d": {
                final CurrentBlob currentBlob = currentBlobMap.get(uuThreadId);
                if (currentBlob == null) {
                    log.error("Current blob for {} not present.", uuThreadId);
                    break;
                }
                if (currentBlob.getFound() != null) {
                    break;
                }
                final TemporaryBlob temporaryBlob = currentBlob.getTemporaryBlob();
                if (temporaryBlob == null) {
                    final String msg = "{}: Blob is not open";
                    log.error(msg, line);
                    throw new IllegalStateException(msg);
                }
                final OutputStream currentBlobFos = currentBlob.getTemporaryBlob().getOutputStream();
                try {
                    if (raw) {
                        currentBlobFos.write(value);
                    } else {
                        StringTokenizer tokens = new StringTokenizer(new String(value));
                        currentBlobFos.write(b64.decode(tokens.nextToken()));
                    }
                } catch (IOException e) {
                    final String msg = "Unable to write blob " + currentBlob.getRef();
                    log.error(msg);
                    throw new IllegalStateException(msg);
                }
                break;
            }

            case "b64!": {
                final CurrentBlob currentBlob = currentBlobMap.remove(uuThreadId);
                if (currentBlob.getFound() != null) {
                    try {
                        currentBlob.getFound().close();
                    } catch (IOException e) {
                        // ignore
                    }
                    break;
                }
                final long b64BlobStart = System.nanoTime();
                final String b64ExpectedRef = currentBlob.getRef();
                Future<?> f = threads.submit(() -> {
                    final TemporaryBlob temporaryBlob = currentBlob.getTemporaryBlob();
                    if (temporaryBlob == null) {
                        final String msg = "Blob is not open";
                        log.error(msg);
                        throw new IllegalStateException(msg);
                    }
                    try {
                        // All refs are 64-char SHA-256 hashes (both segments and data blobs),
                        // so always store under the pre-announced ref.
                        store.putTempBlobAs(currentBlob.getRef(), temporaryBlob);
                    } catch (IOException e) {
                        log.error(e.getMessage());
                    }
                });
                pendingTasks.add(f);
                try {
                    // Wait only for the local write to complete (fast) — the remote/Azure
                    // upload is now submitted asynchronously inside SimpleRemoteBlobStore and
                    // will be waited on by the journal barrier via store.flushPendingWrites().
                    long waitStart = System.nanoTime();
                    f.get();
                    long totalMs = (System.nanoTime() - b64BlobStart) / 1_000_000;
                    long waitMs = (System.nanoTime() - waitStart) / 1_000_000;
                    log.debug("b64! blob={} local write done in {}ms (total {}ms)", b64ExpectedRef, waitMs, totalMs);
                } catch (Exception e) {
                    log.error("Error storing blob {}: {}", b64ExpectedRef, e.getMessage());
                }
                break;
            }

            case "journal":
                synchronized (this) {
                    long journalStart = System.nanoTime();
                    synchronized (pendingTasks) {
                        int taskCount = pendingTasks.size();
                        for (Future<?> f : pendingTasks) {
                            try {
                                f.get();
                            } catch (Exception e) {
                                log.error(e.getMessage() + " while waiting for pending tasks to complete");
                            }
                        }
                        pendingTasks.clear();
                        long localBarrierMs = (System.nanoTime() - journalStart) / 1_000_000;
                        log.debug("journal: local writes done in {}ms ({} tasks)", localBarrierMs, taskCount);
                    }
                    // Wait for all async remote (Azure) uploads submitted by SimpleRemoteBlobStore
                    try {
                        store.flushPendingWrites();
                    } catch (IOException e) {
                        log.error("journal: error flushing remote writes: {}", e.getMessage());
                    }
                    long barrierMs = (System.nanoTime() - journalStart) / 1_000_000;
                    log.info("journal barrier: all blobs remote-durable in {}ms", barrierMs);
                    StringTokenizer tokens = new StringTokenizer(new String(value));
                    final String journalId = tokens.nextToken();
                    final String head = tokens.nextToken();
                    final String oldHead = tokens.nextToken();
                    final TemporaryBlob journalBlob = store.getTempBlob();
                    try (OutputStream journalFile = journalBlob.getOutputStream()) {
                        IOUtils.writeString(journalFile, head);
                        long writeStart = System.nanoTime();
                        store.putTempBlobAs("journal-" + journalId, journalBlob);
                        log.info("journal write(journal-{}) head={} in {}ms, total journal time={}ms",
                                journalId, head,
                                (System.nanoTime() - writeStart) / 1_000_000,
                                (System.nanoTime() - journalStart) / 1_000_000);
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                    if (journalPublisher != null) {
                        journalPublisher.sendMore(JOURNAL_TOPIC);
                        journalPublisher.sendMore(journalId);
                        journalPublisher.sendMore(head);
                        journalPublisher.send(oldHead);
                    }
                    break;
                }

            default: {
                log.warn("Unrecognised op at line {}: {}/{}/{}", line, uuThreadId, op, value);
            }
        }
    }

}
