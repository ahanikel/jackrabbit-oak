/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.store.zeromq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.util.function.Consumer;

public abstract class ZeroMQBackendStore implements BackendStore {

    public static final Logger log = LoggerFactory.getLogger(ZeroMQBackendStore.class);
    public static final String ZEROMQ_READER_PORT = "ZEROMQ_READER_PORT";
    public static final String ZEROMQ_WRITER_PORT = "ZEROMQ_WRITER_PORT";

    protected Thread nodeDiffHandler;
    protected NodeStateAggregator nodeStateAggregator;
    protected Consumer<String> eventWriter;

    final ZMQ.Context context;

    final ZMQ.Poller pollerItems;

    /**
     * read segments to be persisted from this socket
     */
    protected final ZMQ.Socket writerService;

    /**
     * the segment reader service serves segments by id
     */
    protected final ZMQ.Socket readerService;

    /**
     * the thread which listens on the sockets and processes messages
     */
    private final Thread socketHandler;

    private int readerPort;

    private int writerPort;

    public ZeroMQBackendStore(NodeStateAggregator nodeStateAggregator) {
        this.eventWriter = null;
        this.nodeStateAggregator = nodeStateAggregator;
        nodeDiffHandler = new Thread(nodeStateAggregator, "ZeroMQBackendStore NodeStateAggregator");
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
                        log.error(t.toString());
                    }
                }
            }
        };
    }

    public void setEventWriter(Consumer<String> eventWriter) {
        this.eventWriter = eventWriter;
    }

    @Override
    public void handleReaderService(String msg) {
        String ret = null;
        if (msg.startsWith("journal ")) {
            final String instance = msg.substring("journal ".length());
            ret = nodeStateAggregator.getJournalHead(instance);
        } else {
            final ZeroMQNodeState nodeState = nodeStateAggregator.getNodeStore().readNodeState(msg);
            ret = nodeState.getSerialised();
        }
        if (ret != null) {
            readerService.send(ret);
        } else {
            readerService.send("Node not found");
            log.error("Requested node not found: {}", msg);
        }
    }

    @Override
    public void handleWriterService(String msg) {
        eventWriter.accept(msg);
        writerService.send("confirmed");
    }

    private void startBackgroundThreads() {
        if (nodeDiffHandler != null) {
            nodeDiffHandler.start();
            while (!nodeStateAggregator.hasCaughtUp()) {
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

    @Override
    public void open() {
        readerService.bind("tcp://*:" + (readerPort));
        writerService.bind("tcp://*:" + (writerPort));
        pollerItems.register(readerService, ZMQ.Poller.POLLIN);
        pollerItems.register(writerService, ZMQ.Poller.POLLIN);
        startBackgroundThreads();
    }

    @Override
    public void close() {
        stopBackgroundThreads();
        pollerItems.close();
        writerService.close();
        readerService.close();
        context.close();
    }
}
