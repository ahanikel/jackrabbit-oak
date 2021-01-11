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

import org.apache.jackrabbit.oak.store.zeromq.RecordHandler;
import org.apache.jackrabbit.oak.store.zeromq.ZeroMQNodeStore;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.UUID;

public class KafkaNodeStateAggregator implements org.apache.jackrabbit.oak.store.zeromq.NodeStateAggregator {

    private static final String TOPIC = "nodestates";
    private static final Logger log = LoggerFactory.getLogger(KafkaNodeStateAggregator.class);

    private final RecordHandler recordHandler;
    private final KafkaConsumer<String, String> consumer;
    private Iterator<ConsumerRecord<String, String>> records;
    private volatile boolean caughtup;

    public KafkaNodeStateAggregator(String instance) {
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
        recordHandler.setOnCommit(() -> {
            try {
                consumer.commitSync();
            } catch (Exception e) {
            }
        });
        recordHandler.setOnNode(() -> {
            try {
                consumer.commitSync();
            } catch (Exception e) {}
        });
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

    @Override
    public boolean hasCaughtUp() {
        return caughtup;
    }

    @Override
    public ZeroMQNodeStore getNodeStore() {
        return recordHandler.getNodeStore();
    }

    @Override
    public String getJournalHead(String journalName) {
        final String ret = recordHandler.getJournalHead(journalName);
        if (ret == null) {
            return "undefined";
        }
        return ret;
    }

    private static class HandleRebalance implements ConsumerRebalanceListener {
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            // Implement what you want to do once rebalancing is done.
        }

        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            // commit current method
        }
    }
}
