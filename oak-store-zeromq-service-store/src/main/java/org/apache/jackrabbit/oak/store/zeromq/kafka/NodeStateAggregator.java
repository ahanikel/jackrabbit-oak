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

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.store.zeromq.ZeroMQNodeState;
import org.apache.jackrabbit.oak.store.zeromq.ZeroMQNodeStore;
import org.apache.jackrabbit.oak.store.zeromq.ZeroMQPropertyState;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

/*
  R: f8a343af-8dcf-44cd-b4d7-7be5b31faf30
  n^ root bcc37746-ea77-724e-b585-92e7da302ece e69af743-6741-4a78-8d3b-8893a2d1f685
  n^ jcr:system 399e10ca-3fd9-218c-356e-63e8a761d7cf 665fd063-9c24-49db-b24a-a5bd70b5dd2b
  n^ rep:namespaces 5b843d85-a0de-1336-dfdd-9e27bc40506a 074b33a7-b1fb-457d-8bcd-b31697b3d7e3
  p+ p <STRING> = n
  n^ rep:nsdata db69e013-666b-f577-76a8-d3917cdce8f2 778318ac-d634-45ad-a435-436b46dabd7e
  p^ rep:prefixes <STRINGS> = [,p,sv,xml,nt,jcr,oak,rep,mix]
  p^ rep:uris <STRINGS> = [,http://jackrabbit.apache.org/oak/ns/1.0,internal,http://www.jcp.org/jcr/sv/1.0,http://www.jcp.org/jcr/mix/1.0,http://www.jcp.org/jcr/1.0,http://www.w3.org/XML/1998/namespace,http://www.jcp.org/jcr/nt/1.0,n]
  p+ n <STRING> = p
  n!
  n!
  n!
  n!
  R!
  R: 0de0edf8-1a46-46c6-bf48-e0e78164e3af
  n^ root 15230b3a-8146-bfd3-9930-afdb4edf059f bcc37746-ea77-724e-b585-92e7da302ece
  n^ jcr:system 186c8562-594e-a188-0202-1c07b544a2b6 399e10ca-3fd9-218c-356e-63e8a761d7cf
  n^ rep:namespaces bf06a16c-2ad3-96a8-18ca-4dbb1109e453 5b843d85-a0de-1336-dfdd-9e27bc40506a
  p+ p2 <STRING> = n2
  n^ rep:nsdata 555db196-7774-d3ce-2fce-4951140ed73b db69e013-666b-f577-76a8-d3917cdce8f2
  p^ rep:prefixes <STRINGS> = [,p,p2,sv,xml,nt,jcr,oak,rep,mix]
  p^ rep:uris <STRINGS> = [,http://jackrabbit.apache.org/oak/ns/1.0,internal,http://www.jcp.org/jcr/sv/1.0,n2,http://www.jcp.org/jcr/mix/1.0,http://www.jcp.org/jcr/1.0,http://www.w3.org/XML/1998/namespace,http://www.jcp.org/jcr/nt/1.0,n]
  p+ n2 <STRING> = p2
  n!
  n!
  n!
  n!
  R!
*/

public class NodeStateAggregator implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private final ZeroMQNodeStore nodeStore;
    private Iterator<ConsumerRecord<String, String>> records;
    private volatile boolean caughtup;

    public NodeStateAggregator() {
        nodeStore = new ZeroMQNodeStore();
        // It's about time the ZeroMQNodeStore gets its own builder...
        nodeStore.setClusterInstances(1);
        nodeStore.setRemoteReads(false);
        nodeStore.setWriteBackJournal(false);
        nodeStore.setWriteBackNodes(false);
        nodeStore.init();
        caughtup = false;

        // Kafka consumer
        final Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("nodestates"));
        records = null;
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
        List<String> nodeUuids = new ArrayList<>();
        List<NodeBuilder> builders = new ArrayList<>();

        while (true) {
            ConsumerRecord<String, String> rec = nextRecord();
            StringTokenizer tokens = new StringTokenizer(rec.value());

            if (rec.key() == null) {
                continue;
            }

            switch (rec.key()) {
                case "R:": {
                    if (nodeUuids.size() != 0) {
                        throw new IllegalStateException("rootUuid is not null");
                    }
                    nodeUuids.add(tokens.nextToken());
                    if (builders.size() != 0) {
                        throw new IllegalStateException("builders.size() is not 0");
                    }
                    final String baseUuid = tokens.nextToken();
                    final ZeroMQNodeState root = (ZeroMQNodeState) nodeStore.getRoot();
                    if (!baseUuid.equals(root.getUuid())) {
                        throw new IllegalStateException("Base root state is not the expected one");
                    }
                    builders.add(root.builder());
                    break;
                }

                case "R!": {
                    if (builders.size() != 1) {
                        throw new IllegalStateException("builders.size() is not 1");
                    }
                    final NodeBuilder rootBuilder = builders.remove(0);
                    final NodeState rootState = rootBuilder.getNodeState();
                    final ZeroMQNodeState baseState = (ZeroMQNodeState) rootBuilder.getBaseState();
                    final ZeroMQNodeState zmqRootState = nodeStore.mergeSuperRoot(rootState, baseState);
                    if (nodeUuids.size() != 1 || !nodeUuids.get(0).equals(zmqRootState.getUuid())) {
                        throw new IllegalStateException("new uuid is not the expected one");
                    }
                    nodeUuids = null;
                    // commented out to force restarts from the beginning
                    // consumer.commitSync();
                }

                case "n+": {
                    if (builders.size() < 1) {
                        throw new IllegalStateException();
                    }
                    final NodeBuilder parentBuilder = builders.get(builders.size() - 1);
                    final String name = tokens.nextToken();
                    nodeUuids.add(tokens.nextToken());
                    builders.add(parentBuilder.child(name));
                    break;
                }

                case "n^": {
                    final NodeBuilder parentBuilder = builders.get(builders.size() - 1);
                    final String name = tokens.nextToken();
                    nodeUuids.add(tokens.nextToken());
                    builders.add(parentBuilder.getChildNode(name));
                    break;
                }

                case "n-": {
                    final NodeBuilder parentBuilder = builders.get(builders.size() - 1);
                    final String name = tokens.nextToken();
                    parentBuilder.getChildNode(name).remove();
                    break;
                }

                case "n!": {
                    builders.remove(builders.size() - 1);
                    // we're not checking the uuid here
                    // it should be enough to check at the root level
                    // (except for better error handling)
                    nodeUuids.remove(nodeUuids.size() - 1);
                    break;
                }

                case "p+":
                case "p^": {
                    final NodeBuilder parentBuilder = builders.get(builders.size() - 1);
                    final ZeroMQPropertyState ps;
                    try {
                        ps = ZeroMQPropertyState.deSerialise(nodeStore, rec.value());
                    } catch (ZeroMQPropertyState.ParseFailure parseFailure) {
                        throw new IllegalStateException(parseFailure);
                    }
                    parentBuilder.setProperty(ps);
                    break;
                }

                case "p-": {
                    final NodeBuilder parentBuilder = builders.get(builders.size() - 1);
                    final String name = tokens.nextToken();
                    parentBuilder.removeProperty(name);
                    break;
                }
            }
        }
    }

    public ZeroMQNodeState readNodeState(String uuid) {
        return nodeStore.readNodeState(uuid);
    }

    public boolean hasCaughtUp() {
        return caughtup;
    }
}
