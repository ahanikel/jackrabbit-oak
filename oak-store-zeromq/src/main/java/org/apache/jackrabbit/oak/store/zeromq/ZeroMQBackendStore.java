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

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public abstract class ZeroMQBackendStore implements BackendStore {

    public static final Logger log = LoggerFactory.getLogger(ZeroMQBackendStore.class);
    public static final String ZEROMQ_READER_URL = "ZEROMQ_READER_URL";
    public static final String ZEROMQ_WRITER_URL = "ZEROMQ_WRITER_URL";
    public static final String ZEROMQ_NTHREADS   = "ZEROMQ_NTHREADS";

    public static abstract class Builder {
        private NodeStateAggregator nodeStateAggregator;
        private String readerUrl;
        private String writerUrl;
        private int nThreads;

        protected Builder() {
            initFromEnvironment();
        }

        public NodeStateAggregator getNodeStateAggregator() {
            return nodeStateAggregator;
        }

        public Builder withNodeStateAggregator(NodeStateAggregator nodeStateAggregator) {
            this.nodeStateAggregator = nodeStateAggregator;
            return this;
        }

        public String getReaderUrl() {
            return readerUrl;
        }

        public Builder withReaderUrl(String readerUrl) {
            this.readerUrl = readerUrl;
            return this;
        }

        public String getWriterUrl() {
            return writerUrl;
        }

        public Builder withWriterUrl(String writerUrl) {
            this.writerUrl = writerUrl;
            return this;
        }

        public int getNumThreads() {
            return nThreads;
        }

        public Builder withNumThreads(int nThreads) {
            this.nThreads = nThreads;
            return this;
        }

        public abstract ZeroMQBackendStore build() throws FileNotFoundException;

        public Builder initFromEnvironment() {
            readerUrl = System.getenv(ZEROMQ_READER_URL);
            if (readerUrl == null) {
                readerUrl = "tcp://*:8000";
            }
            writerUrl = System.getenv(ZEROMQ_WRITER_URL);
            if (writerUrl == null) {
                writerUrl = "tcp://*:8001";
            }
            try {
                nThreads = Integer.parseInt(System.getenv(ZEROMQ_NTHREADS));
            } catch (NumberFormatException e) {
                nThreads = 4;
            }
            return this;
        }
    }

    protected Thread nodeDiffHandler;
    protected Builder builder;
    protected Consumer<String> eventWriter;

    final ZContext context;

    private final Executor threadPool;

    private final ZMQ.Socket readerFrontend;

    private final ZMQ.Socket readerBackend;

    private final ZMQ.Socket writerFrontend;

    private final ZMQ.Socket writerBackend;

    private final Map<String, String> journalHeads = new ConcurrentHashMap();

    public ZeroMQBackendStore(Builder builder) {
        this.eventWriter = null;
        this.builder = builder;
        nodeDiffHandler = new Thread(builder.getNodeStateAggregator(), "ZeroMQBackendStore NodeStateAggregator");

        context = new ZContext();
        threadPool = Executors.newFixedThreadPool(2 * builder.getNumThreads() + 2);
        readerFrontend = context.createSocket(SocketType.ROUTER);
        readerBackend  = context.createSocket(SocketType.DEALER);
        writerFrontend = context.createSocket(SocketType.ROUTER);
        writerBackend  = context.createSocket(SocketType.DEALER);
        readerFrontend.bind(builder.getReaderUrl());
        readerBackend.bind("inproc://readerBackend");
        writerFrontend.bind(builder.getWriterUrl());
        writerBackend.bind("inproc://writerBackend");
        threadPool.execute(() -> ZMQ.proxy(readerFrontend, readerBackend, null));
        threadPool.execute(() -> ZMQ.proxy(writerFrontend, writerBackend, null));
        for (int nThread = 0; nThread < builder.getNumThreads(); ++nThread) {
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
        for (int nThread = 0; nThread < builder.getNumThreads(); ++nThread) {
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
            ret = builder.getNodeStateAggregator().getJournalHead(instance);
        } else if (msg.startsWith("blob ")) {
            byte[] buffer = new byte[1024 * 1024]; // not final because of fear it's not being GC'd
            try {
                final Blob blob = builder.getNodeStateAggregator().getBlob(msg.substring("blob ".length()));
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
            ret = builder.getNodeStateAggregator().readNodeState(msg);
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
        StringTokenizer t = new StringTokenizer(msg);
        String threadId = t.nextToken();
        String op = t.nextToken();
        if (op.equals("journal")) {
            String journalId = t.nextToken();
            String newHead = t.nextToken();
            String oldHead = t.nextToken();
            synchronized (journalHeads) {
                String refHead = journalHeads.get(journalId);
                if (refHead == null) {
                    refHead = builder.getNodeStateAggregator().getJournalHead(journalId);
                    journalHeads.put(journalId, refHead);
                }
                if (refHead != null && !refHead.equals(oldHead)) {
                    socket.send("refused");
                    return;
                }
                journalHeads.put(journalId, newHead);
            }
        }
        eventWriter.accept(msg);
        socket.send("confirmed");
    }

    private void startBackgroundThreads() {
        if (nodeDiffHandler != null) {
            nodeDiffHandler.start();
            while (!builder.getNodeStateAggregator().hasCaughtUp()) {
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
    }
}
