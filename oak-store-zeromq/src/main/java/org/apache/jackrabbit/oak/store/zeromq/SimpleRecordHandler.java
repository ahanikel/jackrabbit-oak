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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SimpleRecordHandler {

    private static final Logger log = LoggerFactory.getLogger(SimpleRecordHandler.class);
    private static final String JOURNAL_TOPIC = SimpleRequestResponse.Topic.JOURNAL.toString();

    private static class CurrentBlob {
        private String ref;
        private File file;
        private FileInputStream found;
        private FileOutputStream fos;

        public CurrentBlob() {}

        public String getRef() {
            return ref;
        }

        public void setRef(String ref) {
            this.ref = ref;
        }

        public File getFile() {
            return file;
        }

        public void setFile(File file) {
            this.file = file;
        }

        public FileInputStream getFound() {
            return found;
        }

        public void setFound(FileInputStream found) {
            this.found = found;
        }

        public FileOutputStream getFos() {
            return fos;
        }

        public void setFos(FileOutputStream fos) {
            this.fos = fos;
        }
    }

    private final Base64.Decoder b64 = Base64.getDecoder();
    private int line = 0;
    private final SimpleBlobStore store;
    private final Map<String, SimpleMutableNodeState> nodeStates;
    private final Map<String, CurrentBlob> currentBlobMap;
    private final Cache<String, SimpleMutableNodeState> cache;
    private final Cache<String, Long> lastMessageSeen;
    private final ZMQ.Socket journalPublisher;

    public SimpleRecordHandler(SimpleBlobStore store, ZMQ.Socket journalPublisher) {
        this.store = store;
        nodeStates = new HashMap<>();
        currentBlobMap = new HashMap<>();
        cache = CacheBuilder.newBuilder().maximumSize(1000).build();
        lastMessageSeen = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();
        this.journalPublisher = journalPublisher;
    }

    public synchronized void handleRecord(String uuThreadId, long msgid, String op, byte[] value) {

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

        if (op == null) {
            return;
        }

        boolean raw = false;

        switch (op) {
            case "n:": {
                StringTokenizer tokens = new StringTokenizer(new String(value));
                final String newUuid = tokens.nextToken();
                SimpleMutableNodeState newNode;
                if (cache.getIfPresent(newUuid) != null || store.hasBlob(newUuid)) {
                    newNode = new SimpleMutableNodeState(newUuid, true);
                } else {
                    final String oldId = tokens.nextToken();
                    final SimpleMutableNodeState oldNode;
                    if (oldId.equals(SimpleNodeState.UUID_NULL.toString())) {
                        oldNode = new SimpleMutableNodeState(oldId);
                    } else {
                        try {
                            oldNode = cache.get(oldId, () -> {
                                try (FileInputStream oldNodeSer = store.getInputStream(oldId)) {
                                    try {
                                        return SimpleMutableNodeState.deserialise(oldId, oldNodeSer);
                                    } catch (IOException e) {
                                        log.error(e.getMessage() + " when trying to fetch node " + oldId);
                                        throw new IllegalStateException(e);
                                    }
                                } catch (IOException e) {
                                    log.error("Node not found: " + oldId);
                                    throw new IllegalStateException(e);
                                }
                            });
                        } catch (ExecutionException e) {
                            throw new IllegalStateException(e);
                        }
                    }
                    newNode = oldNode.clone(newUuid);
                    newNode.setMsgIdLastSeen(msgid);
                }
                nodeStates.put(uuThreadId, newNode);
                break;
            }

            case "n!": {
                final SimpleMutableNodeState ns = nodeStates.remove(uuThreadId);
                if (ns == null) {
                    log.error("Current nodestate not present");
                    break;
                }
                if (!ns.skip) {
                    try {
                        final File tempFile = store.getTempFile();
                        try (OutputStream os = new FileOutputStream(tempFile)) {
                            ns.serialise(os);
                        }
                        String newRef;
                        try {
                            newRef = store.putTempFile(tempFile);
                        } catch (BlobAlreadyExistsException e) {
                            newRef = e.getRef();
                        }
                        if (!newRef.equals(ns.getUuid())) {
                            // TODO: should we just warn and continue here?
                            throw new IllegalStateException(
                                String.format("Calculated ref %1$s differs from expected ref %2$s",
                                    newRef, ns.getUuid()));
                        }
                        cache.put(newRef, ns);
                    } catch (IOException e) {
                        log.error(e.getMessage() + " while trying to write node " + ns.getUuid());
                        break;
                    }
                }
                break;
            }

            case "n+":
            case "n^": {
                StringTokenizer tokens = new StringTokenizer(new String(value));
                final String name = tokens.nextToken();
                final String uuid = tokens.nextToken();
                final SimpleMutableNodeState parent = nodeStates.get(uuThreadId);
                if (parent == null) {
                    log.error("Current nodestate not present");
                    break;
                }
                if (!parent.skip) {
                    parent.setChild(name, uuid);
                }
                break;
            }

            case "n-": {
                StringTokenizer tokens = new StringTokenizer(new String(value));
                final String name = tokens.nextToken();
                final SimpleMutableNodeState parent = nodeStates.get(uuThreadId);
                if (parent == null) {
                    log.error("Current nodestate not present");
                    break;
                }
                if (!parent.skip) {
                    parent.removeChild(name);
                }
                break;
            }

            case "p+":
            case "p^": {
                String stringValue = new String(value);
                StringTokenizer tokens = new StringTokenizer(stringValue);
                final SimpleMutableNodeState ns = nodeStates.get(uuThreadId);
                if (ns == null) {
                    log.error("Current nodestate not present");
                    break;
                }
                if (!ns.skip) {
                    ns.setProperty(tokens.nextToken(), stringValue.substring(stringValue.indexOf(' ') + 1));
                }
                break;
            }

            case "p-": {
                StringTokenizer tokens = new StringTokenizer(new String(value));
                final String name = tokens.nextToken();
                final SimpleMutableNodeState ns = nodeStates.get(uuThreadId);
                if (ns == null) {
                    log.error("Current nodestate not present");
                    break;
                }
                if (!ns.skip) {
                    ns.removeProperty(name);
                }
                break;
            }

            case "b64+": {
                StringTokenizer tokens = new StringTokenizer(new String(value));
                final String ref = tokens.nextToken();
                CurrentBlob currentBlob = currentBlobMap.get(uuThreadId);
                if (currentBlob == null) {
                    currentBlob = new CurrentBlob();
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
                try {
                    currentBlob.setFound(store.getInputStream(ref));
                    // the blob exists already if no exception occurred
                } catch (FileNotFoundException e) {
                    // the blob doesn't exist yet, build it
                    currentBlob.setRef(ref);
                    for (int i = 0; ; ++i) {
                        try {
                            File tempFile = store.getTempFile();
                            currentBlob.setFile(tempFile);
                            currentBlob.setFos(new FileOutputStream(tempFile));
                            break;
                        } catch (IOException ioe) {
                            if (i % 600 == 0) {
                                log.error("Unable to create temp file, retrying every 100ms (#{}): {}", i, e.getMessage());
                            }
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException interruptedException) {
                                log.info(interruptedException.getMessage());
                                break;
                            }
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
                final OutputStream currentBlobFos = currentBlob.getFos();
                if (currentBlobFos != null) {
                    try {
                        currentBlobFos.close();
                    } catch (IOException e) {
                        log.warn(e.getMessage());
                    }
                    currentBlob.setFos(null);
                }
                final File currentBlobFile = currentBlob.getFile();
                if (currentBlobFile != null) {
                    currentBlobFile.delete();
                    currentBlob.setFile(null);
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
                final OutputStream currentBlobFos = currentBlob.getFos();
                if (currentBlobFos == null) {
                    final String msg = "{}: Blob is not open";
                    log.error(msg, line);
                    throw new IllegalStateException(msg);
                }
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
                final OutputStream currentBlobFos = currentBlob.getFos();
                if (currentBlobFos == null) {
                    final String msg = "Blob is not open";
                    log.error(msg);
                    throw new IllegalStateException(msg);
                }
                try {
                    currentBlobFos.close();
                    String newRef;
                    try {
                        newRef = store.putTempFile(currentBlob.getFile());
                    } catch (BlobAlreadyExistsException e) {
                        newRef = e.getRef();
                    }
                    if (!newRef.equals(currentBlob.getRef())) {
                        log.error("Calculated ref {} differs from expected ref {}", newRef, currentBlob.getRef());
                    }
                } catch (IOException e) {
                    log.error(e.getMessage());
                    throw new IllegalStateException(e);
                }
                break;
            }

            case "journal": {
                StringTokenizer tokens = new StringTokenizer(new String(value));
                final String journalId = tokens.nextToken();
                final String head = tokens.nextToken();
                final String oldHead = tokens.nextToken();
                try (OutputStream journalFile = new FileOutputStream(store.getSpecificFile("journal-" + journalId))) {
                    IOUtils.writeString(journalFile, head);
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

    private static class SimpleMutableNodeState {
        private final String uuid;
        private Map<String, String> children;
        private Map<String, String> properties;
        private volatile boolean serialised = false;
        private final boolean skip;
        private long msgIdLastSeen;

        private SimpleMutableNodeState(String uuid) {
            this.uuid = uuid;
            this.children = new HashMap<>();
            this.properties = new HashMap<>();
            this.skip = false;
        }

        private SimpleMutableNodeState(String uuid, Map<String, String> children, Map<String, String> properties) {
            this.uuid = uuid;
            this.children = children;
            this.properties = properties;
            this.skip = false;
        }

        private SimpleMutableNodeState(String uuid, boolean skip) {
            this.uuid = uuid;
            this.skip = skip;
            if (skip) {
                serialised = true;
            } else {
                this.children = new HashMap<>();
                this.properties = new HashMap<>();
            }
        }

        private SimpleMutableNodeState clone(String newUuid) {
            SimpleMutableNodeState ret = new SimpleMutableNodeState(newUuid);
            ret.children = new HashMap<>();
            ret.children.putAll(this.children);
            ret.properties = new HashMap<>();
            ret.properties.putAll(this.properties);
            return ret;
        }

        private void serialise(OutputStream os) throws IOException {
            if (!serialised) {
                synchronized (this) {
                    if (!serialised) {
                        SimpleNodeState.serialise(os, children, properties);
                        serialised = true;
                    }
                }
            }
        }

        private static SimpleMutableNodeState deserialise(String uuid, InputStream is) throws IOException {
            final Pair<Map<String, String>, Map<String, String>> pair = SimpleNodeState.deserialise(is);
            return new SimpleMutableNodeState(uuid, pair.fst, pair.snd);
        }

        private void setChild(String name, String uuid) {
            checkImmutable();
            children.put(name, uuid);
        }

        private void removeChild(String name) {
            checkImmutable();
            children.remove(name);
        }

        private void setProperty(String name, String property) {
            checkImmutable();
            properties.put(name, property);
        }

        private void removeProperty(String name) {
            checkImmutable();
            properties.remove(name);
        }

        private String getUuid() {
            return uuid;
        }

        private void checkImmutable() {
            if (false && serialised) {
                throw new IllegalStateException("NodeState is immutable");
            }
        }

        private long getMsgIdLastSeen() {
            return msgIdLastSeen;
        }

        private void setMsgIdLastSeen(long msgIdLastSeen) {
            this.msgIdLastSeen = msgIdLastSeen;
        }
    }
}
