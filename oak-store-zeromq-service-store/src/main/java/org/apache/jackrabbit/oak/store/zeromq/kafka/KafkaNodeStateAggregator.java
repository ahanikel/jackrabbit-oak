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

import org.apache.jackrabbit.oak.store.zeromq.AbstractNodeStateAggregator;
import org.apache.jackrabbit.oak.store.zeromq.SimpleRecordHandler;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;

public class KafkaNodeStateAggregator extends AbstractNodeStateAggregator {

    private static final String TOPIC = System.getenv("KAFKA_TOPIC");
    private static final Logger log = LoggerFactory.getLogger(KafkaNodeStateAggregator.class);

    private final KafkaConsumer<String, String> consumer;
    private Iterator<ConsumerRecord<String, String>> records;

    public KafkaNodeStateAggregator(File blobCacheDir) throws IOException {
        super(blobCacheDir);
        caughtup = false;

        // Kafka consumer
        final Properties props = new Properties();
        props.setProperty("bootstrap.servers", System.getenv("KAFKA_SERVERS"));
        props.setProperty("group.id", UUID.randomUUID().toString());
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("max.poll.interval.ms", "10000000");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC), new HandleRebalance());
        records = null;
        recordHandler = new SimpleRecordHandler(blobCacheDir);
        recordHandler.setOnCommit((commitDescriptor) -> {
            try {
                // TODO: this is quite expensive, perhaps we can commit every second or so instead of
                // after every transaction? Or enable autocommit?
                consumer.commitSync();
            } catch (Exception e) {
            }
        });
        recordHandler.setOnNode(() -> {
        });
    }

    private ConsumerRecord<String, String> nextRecord() {
        if (records == null || !records.hasNext()) {
            records = consumer.poll(Duration.ofMillis(100)).iterator();
        }
        if (records.hasNext()) {
            return records.next();
        }
        // TODO: this doesn't work here, caughtup is signalled right at the start
        if (!caughtup) {
            log.info("We have caught up!");
            caughtup = true;
        }
        while (!records.hasNext()) {
            records = consumer.poll(Duration.ofMillis(100)).iterator();
        }
        return records.next();
    }

    @Override
    public void run() {
        while (true) {
            final ConsumerRecord<String, String> rec = nextRecord();
            final String uuThreadId = rec.key();
            final String op = rec.value().substring(0, rec.value().indexOf(" "));
            final String args = rec.value().substring(rec.value().indexOf(" ") + 1);
            recordHandler.handleRecord(uuThreadId, op, args);
        }
    }

    private static class HandleRebalance implements ConsumerRebalanceListener {
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            // Implement what you want to do once rebalancing is done.
            log.warn("Partitions reassigned");
        }

        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            // commit current method
            log.warn("Partitions revoked");
        }
    }
}
