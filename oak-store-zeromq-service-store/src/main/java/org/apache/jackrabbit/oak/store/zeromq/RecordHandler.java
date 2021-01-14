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
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

public class RecordHandler {

    private static final Logger log = LoggerFactory.getLogger(RecordHandler.class);

    private final ZeroMQNodeStore nodeStore;
    private final List<String> nodeUuids = new ArrayList<>();
    private final List<NodeBuilder> builders = new ArrayList<>();
    private String currentBlobRef = null;
    private File currentBlobFile = null;
    private FileOutputStream currentBlobFos = null;
    private final Base64.Decoder b64 = Base64.getDecoder();
    private final String instance;
    private Runnable onCommit;
    private Runnable onNode;
    private int line = 0;
    private final Map<String, String> heads;

    public RecordHandler(String instance) {
        this.instance = instance;
        this.heads = new ConcurrentHashMap<>();
        nodeStore = new ZeroMQNodeStore("aggregator");
        // It's about time the ZeroMQNodeStore gets its own builder...
        nodeStore.setClusterInstances(1);
        nodeStore.setRemoteReads(false);
        nodeStore.setWriteBackJournal(false);
        nodeStore.setWriteBackNodes(false);
        nodeStore.init();
    }

    public void setOnCommit(Runnable onCommit) {
        this.onCommit = onCommit;
    }

    public void setOnNode(Runnable onNode) {
        this.onNode = onNode;
    }

    public void handleRecord(String key, String value) {

        ++line;
        StringTokenizer tokens = new StringTokenizer(value);

        if (key == null) {
            return;
        }

        switch (key) {
            case "R:": {
                if (nodeUuids.size() != 0) {
                    final String msg = "rootUuid is not null at line " + line;
                    log.error(msg);
                    throw new IllegalStateException(msg);
                }
                final String newUuid = tokens.nextToken();
                nodeUuids.add(newUuid);
                if (builders.size() != 0) {
                    final String msg = "builders.size() is not 0";
                    log.error(msg);
                    throw new IllegalStateException(msg);
                }
                final String baseUuid = tokens.nextToken();
                builders.add(nodeStore.readNodeState(baseUuid).builder());
                break;
            }

            case "R!": {
                if (builders.size() != 1) {
                    final String msg = "builders.size() is not 1";
                    log.error(msg);
                    throw new IllegalStateException(msg);
                }
                final NodeBuilder rootBuilder = builders.remove(0);
                final NodeState rootState = rootBuilder.getNodeState();
                final ZeroMQNodeState baseState = (ZeroMQNodeState) rootBuilder.getBaseState();
                final ZeroMQNodeState zmqRootState = (ZeroMQNodeState) rootState;
                final String nodeUuid = nodeUuids.size() == 1 ? nodeUuids.get(0) : "size is " + nodeUuids.size();
                if (nodeUuids.size() != 1 || !nodeUuid.equals(zmqRootState.getUuid())) {
                    // throw new IllegalStateException("new uuid is not the expected one");
                    log.warn("Expected uuid: %s, actual uuid: %s", nodeUuid, zmqRootState.getUuid());
                }
                nodeUuids.clear();
                if (onCommit != null) {
                    onCommit.run();
                }
                break;
            }

            case "n+": {
                if (builders.size() < 1) {
                    final String msg = "builders is not empty";
                    log.error(msg);
                    throw new IllegalStateException(msg);
                }
                final NodeBuilder parentBuilder = builders.get(builders.size() - 1);
                final String name;
                try {
                    name = SafeEncode.safeDecode(tokens.nextToken());
                } catch (UnsupportedEncodingException e) {
                    log.error(e.getMessage());
                    throw new IllegalStateException(e);
                }
                final String newUuid = tokens.nextToken();
                nodeUuids.add(newUuid);
                builders.add(parentBuilder.child(name));
                break;
            }

            case "n^": {
                final NodeBuilder parentBuilder = builders.get(builders.size() - 1);
                final String name;
                try {
                    name = SafeEncode.safeDecode(tokens.nextToken());
                } catch (UnsupportedEncodingException e) {
                    log.error(e.getMessage());
                    throw new IllegalStateException(e);
                }
                final String newUuid = tokens.nextToken();
                nodeUuids.add(newUuid);
                final ZeroMQNodeBuilder childBuilder = (ZeroMQNodeBuilder) parentBuilder.getChildNode(name);
                final String baseUuid = ((ZeroMQNodeState) childBuilder.getBaseState()).getUuid();
                final String expectedBaseUuid = tokens.nextToken();
                if (!baseUuid.equals(expectedBaseUuid)) {
                    log.error("Expected baseUuid: %s, actual: %s", expectedBaseUuid, baseUuid);
                }
                builders.add(childBuilder);
                break;
            }

            case "n-": {
                final NodeBuilder parentBuilder = builders.get(builders.size() - 1);
                final String name;
                try {
                    name = SafeEncode.safeDecode(tokens.nextToken());
                } catch (UnsupportedEncodingException e) {
                    log.error(e.getMessage());
                    throw new IllegalStateException(e);
                }
                parentBuilder.getChildNode(name).remove();
                break;
            }

            case "n!": {
                final ZeroMQNodeBuilder builder = (ZeroMQNodeBuilder) builders.remove(builders.size() - 1);
                final String nodeUuid = nodeUuids.remove(nodeUuids.size() - 1);
                final String builderUuid = ((ZeroMQNodeState) builder.getNodeState()).getUuid();
                if (!nodeUuid.equals(builderUuid)) {
                    log.warn("Expected uuid: %s, actual uuid: %s", nodeUuid, builderUuid);
                }
                if (onNode != null) {
                    onNode.run();
                }
                break;
            }

            case "p+":
            case "p^": {
                final NodeBuilder parentBuilder = builders.get(builders.size() - 1);
                final ZeroMQPropertyState ps;
                try {
                    ps = ZeroMQPropertyState.deSerialise(nodeStore, value);
                } catch (ZeroMQPropertyState.ParseFailure parseFailure) {
                    log.error(parseFailure.getMessage());
                    throw new IllegalStateException(parseFailure);
                }
                parentBuilder.setProperty(ps);
                break;
            }

            case "p-": {
                final NodeBuilder parentBuilder = builders.get(builders.size() - 1);
                final String name;
                try {
                    name = SafeEncode.safeDecode(tokens.nextToken());
                } catch (UnsupportedEncodingException e) {
                    log.error(e.getMessage());
                    throw new IllegalStateException(e);
                }
                parentBuilder.removeProperty(name);
                break;
            }

            case "b64+": {
                final String ref = tokens.nextToken();
                if (currentBlobRef != null) {
                    final String msg = "Blob " + currentBlobRef + " still open when starting with new blob " + ref;
                    log.error(msg);
                    throw new IllegalStateException(msg);
                }
                currentBlobRef = ref;
                for (int i = 0; ; ++i) {
                    try {
                        File blobDir = new File("/tmp/blobs");
                        if (!blobDir.exists()) {
                            blobDir.mkdirs();
                        }
                        currentBlobFile = File.createTempFile("b64temp", "dat", blobDir);
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
            }

            case "b64x": {
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
                if (currentBlobFos == null) {
                    final String msg = "Blob is not open";
                    log.error(msg);
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

    private String formatError(String baseUuid, String rootUuid) {
        final StringBuilder msg = new StringBuilder();
        msg
                .append("Base root state is not the expected one. ")
                .append("Line: ")
                .append(line)
                .append(", expected: ")
                .append(baseUuid)
                .append(", actual: ")
                .append(rootUuid)
                .append('\n')
                .append('\n')
                .append(nodeStore.readNodeState(rootUuid).getSerialised())
        ;
        return msg.toString();
    }

    public ZeroMQNodeStore getNodeStore() {
        return nodeStore;
    }

    public String getJournalHead(String journalName) {
        return heads.get(journalName);
    }
}
