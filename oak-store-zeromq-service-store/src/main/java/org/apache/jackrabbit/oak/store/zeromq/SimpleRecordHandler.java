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

import org.apache.jackrabbit.oak.api.Blob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleRecordHandler implements RecordHandler {

    private static final Logger log = LoggerFactory.getLogger(SimpleRecordHandler.class);

    private String currentBlobRef = null;
    private File currentBlobFile = null;
    private Blob currentBlobFound = null;
    private FileOutputStream currentBlobFos = null;
    private final Base64.Decoder b64 = Base64.getDecoder();
    private final String instance;
    private Runnable onCommit;
    private Runnable onNode;
    private int line = 0;
    private final Map<String, String> heads;
    private final SimpleNodeStore store;
    private final List<SimpleNodeState> nodeStates;

    public SimpleRecordHandler(String instance) {
        this.instance = instance;
        this.heads = new ConcurrentHashMap<>();
        store = new SimpleNodeStore();
        nodeStates = new ArrayList<>();
    }

    public void setOnCommit(Runnable onCommit) {
        this.onCommit = onCommit;
    }

    public void setOnNode(Runnable onNode) {
        this.onNode = onNode;
    }

    @Override
    public void handleRecord(String key, String value) {

        ++line;
        if (line % 100000 == 0) {
            log.info("We're at line {}, nodes so far: {}", line, store.nodeStore.size());
        }
        StringTokenizer tokens = new StringTokenizer(value);

        if (key == null) {
            return;
        }

        switch (key) {
            case "R:": {
                final String newUuid = tokens.nextToken();
                SimpleNodeState newNode;
                if (store.hasNodeState(newUuid)) {
                    newNode = new SimpleNodeState(newUuid, true);
                } else {
                    final String oldid = tokens.nextToken();
                    final SimpleNodeState oldNode = store.getNodeState(oldid);
                    newNode = oldNode.clone(newUuid);
                }
                nodeStates.clear();
                nodeStates.add(newNode);
                break;
            }

            case "R!": {
                if (nodeStates.size() != 1) {
                    log.error("There should only be one nodestate left");
                }
                final SimpleNodeState ns = nodeStates.remove(0);
                if (!ns.skip) {
                    store.putNodeState(ns);
                }
                if (onCommit != null) {
                    onCommit.run();
                }
                break;
            }

            case "n+": {
                final String name = tokens.nextToken();
                final String uuid = tokens.nextToken();
                final SimpleNodeState child = new SimpleNodeState(uuid, store.hasNodeState(uuid));
                final SimpleNodeState parent = nodeStates.get(nodeStates.size()-1);
                if (!parent.skip) {
                    parent.setChild(name, uuid);
                }
                nodeStates.add(child);
                break;
            }

            case "n^": {
                final String name = tokens.nextToken();
                final String uuid = tokens.nextToken();
                final String oldid = tokens.nextToken();
                SimpleNodeState newNode;
                if (store.hasNodeState(uuid)) {
                    newNode = new SimpleNodeState(uuid, true);
                } else {
                    final SimpleNodeState oldNode = store.getNodeState(oldid);
                    newNode = oldNode.clone(uuid);
                }
                final SimpleNodeState parent = nodeStates.get(nodeStates.size() - 1);
                if (!parent.skip) {
                    parent.setChild(name, uuid);
                }
                nodeStates.add(newNode);
                break;
            }

            case "n-": {
                final String name = tokens.nextToken();
                final SimpleNodeState parent = nodeStates.get(nodeStates.size()-1);
                if (!parent.skip) {
                    parent.removeChild(name);
                }
                break;
            }

            case "n!": {
                final SimpleNodeState ns = nodeStates.remove(nodeStates.size()-1);
                if (!ns.skip) {
                    store.putNodeState(ns);
                }
                if (onNode != null) {
                    onNode.run();
                }
                break;
            }

            case "p+":
            case "p^": {
                final SimpleNodeState ns = nodeStates.get(nodeStates.size()-1);
                if (!ns.skip) {
                    ns.setProperty(tokens.nextToken(), value);
                }
                break;
            }

            case "p-": {
                final String name = tokens.nextToken();
                final SimpleNodeState ns = nodeStates.get(nodeStates.size()-1);
                if (!ns.skip) {
                    ns.removeProperty(name);
                }
                break;
            }

            case "b64+": {
                final String ref = tokens.nextToken();
                if (currentBlobRef != null) {
                    final String msg = "Blob " + currentBlobRef + " still open when starting with new blob " + ref;
                    log.error(msg);
                    throw new IllegalStateException(msg);
                }
                if (currentBlobFound != null) {
                    final String msg = "currentBlobFound is not null";
                    log.error(msg);
                    throw new IllegalStateException(msg);
                }
                currentBlobFound = ZeroMQBlob.newInstance(ref);
                if (currentBlobFound != null) {
                    break;
                }
                currentBlobRef = ref;
                for (int i = 0; ; ++i) {
                    try {
                        File blobDir = new File("/tmp/blobs");
                        if (!blobDir.exists()) {
                            blobDir.mkdirs();
                        }
                        currentBlobFile = File.createTempFile("b64temp", ".dat", blobDir);
                        currentBlobFos = new FileOutputStream(currentBlobFile);
                    } catch (IOException e) {
                        if (i % 600 == 0) {
                            log.error("Unable to create temp file, retrying every 100ms: {}", e.getMessage());
                        }
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException interruptedException) {
                            break;
                        }
                    }
                    break;
                }
                break;
            }

            case "b64x": {
                if (currentBlobFound != null) {
                    currentBlobFound = null;
                    break;
                }
                if (currentBlobFos != null) {
                    try {
                        currentBlobFos.close();
                    } catch (IOException e) {
                        log.warn(e.getMessage());
                    }
                    currentBlobFos = null;
                }
                if (currentBlobFile != null) {
                    currentBlobFile.delete();
                    currentBlobFile = null;
                }
                currentBlobRef = null;
                break;
            }

            case "b64d": {
                if (currentBlobFound != null) {
                    break;
                }
                if (currentBlobFos == null) {
                    final String msg = "{}: Blob is not open";
                    log.error(msg, line);
                    throw new IllegalStateException(msg);
                }
                try {
                    currentBlobFos.write(b64.decode(tokens.nextToken()));
                } catch (IOException e) {
                    final String msg = "Unable to write blob " + currentBlobRef;
                    log.error(msg);
                    throw new IllegalStateException(msg);
                }
                break;
            }

            case "b64!": {
                if (currentBlobFound != null) {
                    currentBlobFound = null;
                    break;
                }
                if (currentBlobFos == null) {
                    final String msg = "Blob is not open";
                    log.error(msg);
                    throw new IllegalStateException(msg);
                }
                try {
                    currentBlobFos.close();
                    currentBlobFos = null;
                    Blob blob = ZeroMQBlob.newInstance(currentBlobRef, currentBlobFile);
                    log.trace("Created new blob {}", blob.getReference());
                    currentBlobFile = null;
                    currentBlobRef = null;
                } catch (IOException e) {
                    log.error(e.getMessage());
                    throw new IllegalStateException(e);
                }
                break;
            }

            case "journal": {
                final String instance = tokens.nextToken();
                final String head = tokens.nextToken();
                heads.put(instance, head);
                break;
            }
        }
    }

    @Override
    public String getJournalHead(String journalName) {
        return heads.get(journalName);
    }

    @Override
    public String readNodeState(String msg) {
        final SimpleNodeState ret = store.getNodeState(msg);
        if (ret != null) {
            return ret.serialise();
        }
        return null;
    }

    @Override
    public Blob getBlob(String reference) {
        return ZeroMQBlob.newInstance(reference);
    }

    private static class SimpleNodeStore {
        private Map<String, SimpleNodeState> nodeStore;

        private SimpleNodeStore() {
            this.nodeStore = new ConcurrentHashMap<>(10000000);
            final String emptyUuid = "00000000-0000-0000-0000-000000000000";
            final SimpleNodeState empty = new SimpleNodeState(emptyUuid);
            empty.makeImmutable();
            this.nodeStore.put(emptyUuid, empty);
        }

        private SimpleNodeState getNodeState(String uuid) {
            return nodeStore.get(uuid);
        }

        private void putNodeState(SimpleNodeState ns) {
            ns.makeImmutable();
            final String uuid = ns.getUuid();
            nodeStore.put(uuid, ns);
            log.trace("Stored {}, size {}", uuid, nodeStore.size());
        }

        public boolean hasNodeState(String uuid) {
            return nodeStore.containsKey(uuid);
        }
    }

    private static class SimpleNodeState {
        private final String uuid;
        private Map<String, String> children;
        private Map<String, String> properties;
        private String serialised = null;
        private boolean skip;

        private SimpleNodeState(String uuid) {
            this.uuid = uuid;
            this.children = new HashMap<>();
            this.properties = new HashMap<>();
            this.skip = false;
        }

        private SimpleNodeState(String uuid, boolean skip) {
            this.uuid = uuid;
            this.skip = skip;
            if (skip) {
                serialised = "skip";
            } else {
                this.children = new HashMap<>();
                this.properties = new HashMap<>();
            }
        }

        private SimpleNodeState clone(String newUuid) {
            SimpleNodeState ret = new SimpleNodeState(newUuid);
            ret.children = new HashMap<>();
            ret.children.putAll(this.children);
            ret.properties = new HashMap<>();
            ret.properties.putAll(this.properties);
            return ret;
        }

        private String serialise() {
            if (serialised == null) {
                serialised = ZeroMQNodeState.serialise2(children, properties);
            }
            return serialised;
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

        private void makeImmutable() {
            checkImmutable();
            serialised = serialise();
        }

        private void checkImmutable() {
            if (serialised != null) {
                throw new IllegalStateException("NodeState is immutable");
            }
        }
    }
}
