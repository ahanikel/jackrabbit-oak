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

import org.apache.jackrabbit.oak.store.zeromq.ZeroMQNodeState;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.zeromq.ZMQ;

import java.util.Properties;

/**
 * A store used for in-memory operations.
 */
public class ZeroMQBackendStore {

    static final String ZEROMQ_READER_PORT = "ZEROMQ_READER_PORT";
    static final String ZEROMQ_WRITER_PORT = "ZEROMQ_WRITER_PORT";
    //private static final int CHUNKSIZE = 256 * 1024;

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
    // private KafkaStreams kafka;
    private KafkaProducer<String, String> producer;
    private Thread nodeDiffHandler;
    private KafkaNodeStateAggregator kafkaNodeStateAggregator;
    /*
    private KTable<String, String> kafkaTable;
    private ReadOnlyKeyValueStore<Object, Object> kafkaStore;
    */

    public ZeroMQBackendStore(String instance) {
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
        kafkaNodeStateAggregator = new KafkaNodeStateAggregator(instance);
        nodeDiffHandler = new Thread(kafkaNodeStateAggregator, "ZeroMQBackendStore NodeStateAggregator");
        socketHandler = new Thread("ZeroMQBackendStore Socket Handler") {
            @Override
            public void run() {
                while (!isInterrupted()) {
                    try {
                        while (readerService.hasReceiveMore()) {
                            readerService.recv();
                        }
                        while (writerService.hasReceiveMore()) {
                            writerService.recv();
                        }
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

        /*
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
        */
    }

    void handleReaderService(String msg) {
        String ret = null;
        if (msg.startsWith("journal ")) {
            final String instance = msg.substring("journal ".length());
            ret = kafkaNodeStateAggregator.getJournalHead(instance);
        } else {
            final ZeroMQNodeState nodeState = kafkaNodeStateAggregator.getNodeStore().readNodeState(msg);
            ret = nodeState.getSerialised();
        }
        if (ret != null) {
            readerService.send(ret);
        } else {
            readerService.send("Node not found");
            System.err.println("Requested node not found: " + msg);
        }
    }

    void handleWriterService(String msg) {
        try {
            final int firstSpace = msg.indexOf(' ');
            String key;
            String value;
            if (firstSpace < 0) {
                key = msg;
                value = "";
            } else {
                key = msg.substring(0, firstSpace);
                value = msg.substring(firstSpace + 1);
            }
            producer.send(new ProducerRecord<>(kafkaTopic, key, value));
        } finally {
            writerService.send("confirmed");
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
        /*
        if (kafka != null) {
            kafka.close();
            kafka = null;
        }
        */
        if (producer != null) {
            producer.close();
            producer = null;
        }
    }

    private void startBackgroundThreads() {
        if (nodeDiffHandler != null) {
            nodeDiffHandler.start();
            while (!kafkaNodeStateAggregator.hasCaughtUp()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
        if (socketHandler != null) {
            socketHandler.start();
        }
    }

    private void stopBackgroundThreads() {
        if (socketHandler != null) {
            socketHandler.interrupt();
        }
        if (nodeDiffHandler != null) {
            nodeDiffHandler.interrupt();
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java " + ZeroMQBackendStore.class.getCanonicalName() + " <instanceName>");
            System.exit(1);
        }
        final String instance = args[0];
        final ZeroMQBackendStore zeroMQBackendStore = new ZeroMQBackendStore(instance);
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
