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

import org.apache.jackrabbit.oak.api.Blob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.InputStream;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public abstract class ZeroMQBackendStore implements BackendStore {

    public static final Logger log = LoggerFactory.getLogger(ZeroMQBackendStore.class);
    public static final String ZEROMQ_READER_PORT = "ZEROMQ_READER_PORT";
    public static final String ZEROMQ_WRITER_PORT = "ZEROMQ_WRITER_PORT";
    public static final String ZEROMQ_NTHREADS    = "ZEROMQ_NTHREADS";

    protected Thread nodeDiffHandler;
    protected NodeStateAggregator nodeStateAggregator;
    protected Consumer<String> eventWriter;

    final ZContext context;

    private int readerPort;

    private int writerPort;

    private int nThreads;

    private final Executor threadPool;

    private final ZMQ.Socket readerFrontend;

    private final ZMQ.Socket readerBackend;

    private final ZMQ.Socket writerFrontend;

    private final ZMQ.Socket writerBackend;

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
        try {
            nThreads = Integer.parseInt(System.getenv(ZEROMQ_NTHREADS));
        } catch (NumberFormatException e) {
            nThreads = 4;
        }
        context = new ZContext();
        threadPool = Executors.newFixedThreadPool(2 * nThreads + 2);
        readerFrontend = context.createSocket(SocketType.ROUTER);
        readerBackend  = context.createSocket(SocketType.DEALER);
        writerFrontend = context.createSocket(SocketType.ROUTER);
        writerBackend  = context.createSocket(SocketType.DEALER);
        readerFrontend.bind("tcp://*:" + readerPort);
        readerBackend.bind("inproc://readerBackend");
        writerFrontend.bind("tcp://*:" + writerPort);
        writerBackend.bind("inproc://writerBackend");
        threadPool.execute(() -> ZMQ.proxy(readerFrontend, readerBackend, null));
        threadPool.execute(() -> ZMQ.proxy(writerFrontend, writerBackend, null));
        for (int nThread = 0; nThread < nThreads; ++nThread) {
            threadPool.execute(() -> {
                final ZMQ.Socket socket = context.createSocket(SocketType.REP);
                socket.connect("inproc://readerBackend");
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        handleReaderService(socket);
                    } catch (Throwable t) {
                        log.error(t.toString());
                    }
                }
            });
        };
        for (int nThread = 0; nThread < nThreads; ++nThread) {
            threadPool.execute(() -> {
                final ZMQ.Socket socket = context.createSocket(SocketType.REP);
                socket.connect("inproc://writerBackend");
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        handleWriterService(socket);
                    } catch (Throwable t) {
                        log.error(t.toString());
                    }
                }
            });
        };
    }

    public void setEventWriter(Consumer<String> eventWriter) {
        this.eventWriter = eventWriter;
    }

    @Override
    public void handleReaderService(ZMQ.Socket socket) {
        final String msg = socket.recvStr();
        String ret = null;
        if (msg.startsWith("journal ")) {
            final String instance = msg.substring("journal ".length());
            ret = nodeStateAggregator.getJournalHead(instance);
        } else if (msg.startsWith("checkpoints ")) {
            final String instance = msg.substring("checkpoints ".length());
            ret = nodeStateAggregator.getCheckpointHead(instance);
        } else if (msg.startsWith("blob ")) {
            byte[] buffer = new byte[1024*1024];
            try {
                final Blob blob = nodeStateAggregator.getBlob(msg.substring("blob ".length()));
                if (blob == null) {
                    throw new IllegalArgumentException(msg + " not found");
                }
                final InputStream is = blob.getNewStream();
                for (int nBytes = is.read(buffer); nBytes > 0; nBytes = is.read(buffer)) {
                    socket.send(buffer, 0, nBytes, ZMQ.SNDMORE);
                }
            } catch (IllegalArgumentException iae) {
                log.trace(iae.getMessage());
            } catch (Exception ioe) {
                log.error(ioe.getMessage());
            } finally {
                socket.send(new byte[0]);
            }
            return;
        } else {
            ret = nodeStateAggregator.readNodeState(msg);
        }
        if (ret != null) {
            socket.send(ret);
        } else {
            socket.send("Node not found");
            log.error("Requested node not found: {}", msg);
        }
    }

    @Override
    public void handleWriterService(ZMQ.Socket socket) {
        final String msg = socket.recvStr();
        eventWriter.accept(msg);
        socket.send("confirmed");
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
    }

    private void stopBackgroundThreads() {
        if (nodeDiffHandler != null) {
            nodeDiffHandler.interrupt();
        }
    }

    @Override
    public void open() {
        startBackgroundThreads();
    }

    @Override
    public void close() {
        stopBackgroundThreads();
        context.close();
    }
}
