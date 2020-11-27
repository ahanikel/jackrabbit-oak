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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.zeromq.ZMQ;

import java.util.Properties;

/**
 * A store used for in-memory operations.
 */
public class ZeroMQBackendStore {

    static final String ZEROMQ_READER_PORT = "ZEROMQ_READER_PORT";
    static final String ZEROMQ_WRITER_PORT = "ZEROMQ_WRITER_PORT";
    private static final int CHUNKSIZE = 256 * 1024;

    final ZMQ.Context context;

    final ZMQ.Poller pollerItems;

    /**
     * read segments to be persisted from this socket
     */
    final ZMQ.Socket writerService;

    /**
     * the segment reader service serves segments by id
     */
    final ZMQ.Socket readerService;

    /**
     * the thread which listens on the sockets and processes messages
     */
    private final Thread socketHandler;

    private int readerPort;

    private int writerPort;

    private String kafkaTopic;
    private KafkaStreams kafka;
    private KafkaProducer<String, String> producer;
    private KTable<String, String> kafkaTable;
    private ReadOnlyKeyValueStore<Object, Object> kafkaStore;

    public ZeroMQBackendStore() {
        initKafka();
        try {
            readerPort = Integer.parseInt(System.getenv(ZEROMQ_READER_PORT));
        } catch (NumberFormatException e) {
            readerPort = 8000;
        }
        try {
            writerPort = Integer.parseInt(System.getenv(ZEROMQ_WRITER_PORT));
        } catch (NumberFormatException e) {
            writerPort = 8001;
        }
        context = ZMQ.context(20);
        readerService = context.socket(ZMQ.REP);
        writerService = context.socket(ZMQ.REP);
        pollerItems = context.poller(2);
        socketHandler = new Thread("ZeroMQBackendStore Socket Handler") {
            @Override
            public void run() {
                while (!isInterrupted()) {
                    try {
                        pollerItems.poll();
                        if (pollerItems.pollin(0)) {
                            handleReaderService(readerService.recvStr());
                        }
                        if (pollerItems.pollin(1)) {
                            handleWriterService(writerService.recvStr());
                        }
                    } catch (Throwable t) {
                        System.err.println(t.toString());
                    }
                }
            }
        };
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

        final Properties kafkaStreamProperties = new Properties();
        kafkaStreamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "oak-store-kafka");
        kafkaStreamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_SERVERS"));
        kafkaStreamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        kafkaStreamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        final StreamsBuilder builder = new StreamsBuilder();
        final String kafkaStoreName = kafkaTopic + "-store";
        kafkaTable = builder.table(kafkaTopic
                , Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(kafkaStoreName));
        final Topology topology = builder.build();
        kafka = new KafkaStreams(topology, kafkaStreamProperties);
        kafka.start();
        while (kafkaStore == null) {
            try {
                kafkaStore = kafka.store(StoreQueryParameters.fromNameAndType(kafkaStoreName, QueryableStoreTypes.keyValueStore()));
            } catch (InvalidStateStoreException ignored) {
                // store not yet ready for querying
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    void handleReaderService(String msg) {
        final String sNode = (String) kafkaStore.get(msg);
        if (sNode != null) {
            if (sNode.startsWith("begin") || msg.equals("journal")) {
                readerService.send(sNode);
            } else {
                final int chunks = Integer.parseInt(sNode);
                int i;
                for (i = 0; i < chunks - 1; ++i) {
                    readerService.sendMore((String) kafkaStore.get(msg + ":" + i));
                }
                readerService.send((String) kafkaStore.get(msg + ":" + i));
            }
        } else {
            readerService.send("Node not found");
            System.err.println("Requested node not found: " + msg);
        }
    }

    void handleWriterService(String msg) {
        try {
            final int firstLineSep = msg.indexOf('\n');
            final String uuid = msg.substring(0, firstLineSep);
            final String ser =  msg.substring(firstLineSep + 1);
            final int chunks = ser.length() / CHUNKSIZE + 1;
            if (chunks > 1) {
                int i;
                for (i = 0; i < chunks - 1; ++i) {
                        producer.send(new ProducerRecord<>(kafkaTopic, uuid + ":" + i, ser.substring(i * CHUNKSIZE, (i + 1) * CHUNKSIZE)));
                }
                producer.send(new ProducerRecord<>(kafkaTopic, uuid + ":" + i, ser.substring(i * CHUNKSIZE)));
                producer.send(new ProducerRecord<>(kafkaTopic, uuid, "" + chunks));
            } else {
                producer.send(new ProducerRecord<>(kafkaTopic, uuid, ser));
            }
            writerService.send(uuid + " confirmed.");
            if (msg.length() > 1000000) {
                System.err.println("Large message: " + uuid + ": " + msg.length());
            }
        } catch (Exception e) {
            System.err.println(e.toString());
        }
    }

    private void open() {
        readerService.bind("tcp://*:" + (readerPort));
        writerService.bind("tcp://*:" + (writerPort));
        pollerItems.register(readerService, ZMQ.Poller.POLLIN);
        pollerItems.register(writerService, ZMQ.Poller.POLLIN);
        startBackgroundThreads();
    }

    private void close() {
        stopBackgroundThreads();
        pollerItems.close();
        writerService.close();
        readerService.close();
        context.close();
        if (kafka != null) {
            kafka.close();
            kafka = null;
        }
        if (producer != null) {
            producer.close();
            producer = null;
        }
    }

    private void startBackgroundThreads() {
        if (socketHandler != null) {
            socketHandler.start();
        }
    }

    private void stopBackgroundThreads() {
        if (socketHandler != null) {
            socketHandler.interrupt();
        }
    }

    public static void main(String[] args) {
        final ZeroMQBackendStore zeroMQBackendStore = new ZeroMQBackendStore();
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}