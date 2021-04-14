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

import org.apache.jackrabbit.oak.store.zeromq.ZeroMQBackendStore;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

/**
 * A store used for in-memory operations.
 */
public class KafkaBackendStore extends ZeroMQBackendStore {

    private String kafkaTopic;
    private KafkaProducer<String, String> producer;

    public KafkaBackendStore() {
        super(new KafkaNodeStateAggregator());
        setEventWriter(this::writeEvent);
        initKafka();
        open();
    }

    public void finalize() {
        close();
    }

    private void initKafka() {
        kafkaTopic = System.getenv("KAFKA_TOPIC");
        final Properties kafkaProducerProperties = new Properties();
        kafkaProducerProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_SERVERS"));
        kafkaProducerProperties.put("key.serializer", StringSerializer.class);
        kafkaProducerProperties.put("value.serializer", StringSerializer.class);
        producer = new KafkaProducer<String, String>(kafkaProducerProperties);
        // ensure the topic has been created, otherwise the store will just shut down
        producer.send(new ProducerRecord<>(kafkaTopic, "hello", "world"));
    }

    private void writeEvent(String event) {
            final int firstSpace = event.indexOf(' ');
            String key;
            String value;
            if (firstSpace < 0) {
                key = event;
                value = "";
            } else {
                key = event.substring(0, firstSpace);
                value = event.substring(firstSpace + 1);
            }
            producer.send(new ProducerRecord<>(kafkaTopic, key, value));
    }

    @Override
    public void close() {
        super.close();
        if (producer != null) {
            producer.close();
            producer = null;
        }
    }
}
