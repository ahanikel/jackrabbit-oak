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
package org.apache.jackrabbit.oak.store.zeromq.kafka;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.store.zeromq.SafeEncode;
import org.apache.jackrabbit.oak.store.zeromq.ZeroMQBlob;
import org.apache.jackrabbit.oak.store.zeromq.ZeroMQNodeBuilder;
import org.apache.jackrabbit.oak.store.zeromq.ZeroMQNodeState;
import org.apache.jackrabbit.oak.store.zeromq.ZeroMQNodeStore;
import org.apache.jackrabbit.oak.store.zeromq.ZeroMQPropertyState;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.UUID;

public class NodeStateAggregator implements Runnable {

    private static final String TOPIC = "nodestates";
    private static final Logger log = LoggerFactory.getLogger(NodeStateAggregator.class);

    private final RecordHandler recordHandler;
    private final KafkaConsumer<String, String> consumer;
    private Iterator<ConsumerRecord<String, String>> records;
    private volatile boolean caughtup;

    public NodeStateAggregator(String instance) {
        caughtup = false;

        // Kafka consumer
        final Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", UUID.randomUUID().toString());
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("max.poll.interval.ms", "10000000");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC), new HandleRebalance());
        records = null;
        recordHandler = new RecordHandler(instance);
        recordHandler.setOnCommit(() -> consumer.commitSync());
        recordHandler.setOnNode(() -> consumer.commitSync());
    }

    private ConsumerRecord<String, String> nextRecord() {
        if (records == null || !records.hasNext()) {
            records = consumer.poll(Duration.ofMillis(100)).iterator();
        }
        if (records.hasNext()) {
            return records.next();
        }
        caughtup = true;
        while (!records.hasNext()) {
            records = consumer.poll(Duration.ofMillis(100)).iterator();
        }
        return records.next();
    }

    private StringTokenizer tokens(String value) {
        return new StringTokenizer(value);
    }

    @Override
    public void run() {

        while (true) {
            ConsumerRecord<String, String> rec = nextRecord();
            recordHandler.handleRecord(rec.key(), rec.value());
        }
    }

    public boolean hasCaughtUp() {
        return caughtup;
    }

    public ZeroMQNodeStore getNodeStore() {
        return recordHandler.getNodeStore();
    }

    private static class HandleRebalance implements ConsumerRebalanceListener {
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            // Implement what you want to do once rebalancing is done.
        }

        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            // commit current method
        }
    }

    public static class RecordHandler {
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

        public RecordHandler(String instance) {
            this.instance = instance;
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
                        throw new IllegalStateException("rootUuid is not null");
                    }
                    final String newUuid = tokens.nextToken();
                    nodeUuids.add(newUuid);
                    if (builders.size() != 0) {
                        throw new IllegalStateException("builders.size() is not 0");
                    }
                    final String baseUuid = tokens.nextToken();
                    final String rootUuid = nodeStore.readRoot();
                    if (!baseUuid.equals(rootUuid)
                            // special case: empty node store after init:
                            && !"823f2252-db37-b0ca-3f7e-09cd073b530a".equals(rootUuid)) {
                       throw new IllegalStateException("Base root state is not the expected one");
                    }
                    builders.add(nodeStore.getSuperRoot().builder());
                    break;
                }

                case "R!": {
                    if (builders.size() != 1) {
                        throw new IllegalStateException("builders.size() is not 1");
                    }
                    final NodeBuilder rootBuilder = builders.remove(0);
                    final NodeState rootState = rootBuilder.getNodeState();
                    final ZeroMQNodeState baseState = (ZeroMQNodeState) rootBuilder.getBaseState();
                    //final ZeroMQNodeState zmqRootState = nodeStore.mergeSuperRoot(rootState, baseState);
                    final ZeroMQNodeState zmqRootState = (ZeroMQNodeState) rootState;
                    nodeStore.setRoot(zmqRootState.getUuid());
                    final String nodeUuid = nodeUuids.size() == 1 ? nodeUuids.get(0) : "size is " + nodeUuids.size();
                    if (nodeUuids.size() != 1 || !nodeUuid.equals(zmqRootState.getUuid())) {
                        // throw new IllegalStateException("new uuid is not the expected one");
                        log.warn("Expected uuid: {}, actual uuid: {}", nodeUuid, zmqRootState.getUuid());
                        System.err.format("Expected uuid: {}, actual uuid: {}", nodeUuid, zmqRootState.getUuid());
                    }
                    nodeUuids.clear();
                    if (onCommit != null) {
                        onCommit.run();
                    }
                    break;
                }

                case "n+": {
                    if (builders.size() < 1) {
                        throw new IllegalStateException();
                    }
                    final NodeBuilder parentBuilder = builders.get(builders.size() - 1);
                    final String name;
                    try {
                        name = SafeEncode.safeDecode(tokens.nextToken());
                    } catch (UnsupportedEncodingException e) {
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
                        throw new IllegalStateException(e);
                    }
                    final String newUuid = tokens.nextToken();
                    nodeUuids.add(newUuid);
                    final ZeroMQNodeBuilder childBuilder = (ZeroMQNodeBuilder) parentBuilder.getChildNode(name);
                    final String baseUuid = ((ZeroMQNodeState) childBuilder.getBaseState()).getUuid();
                    final String expectedBaseUuid = tokens.nextToken();
                    if (!baseUuid.equals(expectedBaseUuid)) {
                        System.err.format("Expected baseUuid: %s, actual: %s", expectedBaseUuid, baseUuid);
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
                        // throw new IllegalStateException("new uuid is not the expected one");
                        log.warn("Expected uuid: {}, actual uuid: {}", nodeUuid, builderUuid);
                        System.err.format("Expected uuid: %s, actual uuid: %s", nodeUuid, builderUuid);
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
                        throw new IllegalStateException(e);
                    }
                    parentBuilder.removeProperty(name);
                    break;
                }

                case "b64+": {
                    final String ref = tokens.nextToken();
                    if (currentBlobRef != null) {
                        throw new IllegalStateException("Blob " + currentBlobRef + " still open when starting with new blob " + ref);
                    }
                    currentBlobRef = ref;
                    try {
                        File blobDir = new File("/tmp/blobs");
                        if (!blobDir.exists()) {
                            blobDir.mkdirs();
                        }
                        currentBlobFile = File.createTempFile("b64temp", "dat", blobDir);
                        currentBlobFos = new FileOutputStream(currentBlobFile);
                    } catch (IOException e) {
                        log.error("Unable to create temp file, looping forever");
                        while (true) {
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException interruptedException) {
                            }
                        }
                    }
                    break;
                }

                case "b64x": {
                    if (currentBlobFos != null) {
                        try {
                            currentBlobFos.close();
                        } catch (IOException e) {
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
                        throw new IllegalStateException("Blob is not open");
                    }
                    try {
                        currentBlobFos.write(b64.decode(tokens.nextToken()));
                    } catch (IOException e) {
                        throw new IllegalStateException("Unable to write blob " + currentBlobRef);
                    }
                    break;
                }

                case "b64!": {
                    if (currentBlobFos == null) {
                        throw new IllegalStateException("Blob is not open");
                    }
                    try {
                        currentBlobFos.close();
                        currentBlobFos = null;
                        Blob blob = ZeroMQBlob.newInstance(currentBlobRef, currentBlobFile);
                        log.trace("Created new blob {}", blob.getReference());
                        currentBlobFile = null;
                        currentBlobRef = null;
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                    break;
                }

                case "journal": {
                    final String instance = tokens.nextToken();
                    final String head = tokens.nextToken();
                    if (!this.instance.equals(instance)) {
                        break;
                    }
                    nodeStore.setRoot(head);
                    break;
                }
            }
        }

        public ZeroMQNodeStore getNodeStore() {
            return nodeStore;
        }
    }
}
