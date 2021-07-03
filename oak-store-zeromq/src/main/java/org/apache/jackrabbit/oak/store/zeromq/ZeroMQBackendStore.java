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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;
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

        protected Builder() {}

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

    private final Router readerFrontend;

    private final ZMQ.Socket writerFrontend;

    private final ZMQ.Socket writerBackend;

    public ZeroMQBackendStore(Builder builder) {
        this.eventWriter = null;
        this.builder = builder;
        nodeDiffHandler = new Thread(builder.getNodeStateAggregator(), "ZeroMQBackendStore NodeStateAggregator");

        context = new ZContext();
        threadPool = Executors.newFixedThreadPool(2 * builder.getNumThreads() + 2);
        readerFrontend = new Router(context, builder.getReaderUrl(), "inproc://readerBackend");
        writerFrontend = context.createSocket(SocketType.ROUTER);
        writerBackend  = context.createSocket(SocketType.DEALER);
        writerFrontend.bind(builder.getWriterUrl());
        writerBackend.bind("inproc://writerBackend");
        threadPool.execute(() -> ZMQ.proxy(writerFrontend, writerBackend, null));
        for (int nThread = 0; nThread < builder.getNumThreads(); ++nThread) {
            threadPool.execute(() -> {
                log.info(Thread.currentThread().getName());
                final ZMQ.Socket socket = context.createSocket(SocketType.REQ);
                socket.setIdentity(("" + Thread.currentThread().getId()).getBytes());
                socket.connect("inproc://readerBackend");
                socket.sendMore("H");
                socket.send("");
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
                    socket.sendMore("C");
                    socket.send(buffer, 0, nBytes, 0);
                    socket.recv();
                }
                socket.sendMore("E");
                socket.send("");
            } catch (IllegalArgumentException iae) {
                log.trace(iae.getMessage());
            } catch (Exception ioe) {
                log.error(ioe.getMessage());
            } finally {
                return;
            }
        } else {
            ret = builder.getNodeStateAggregator().readNodeState(msg);
        }
        if (ret != null) {
            socket.sendMore("E");
            socket.send(ret);
        } else {
            socket.sendMore("E");
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
            while (!builder.getNodeStateAggregator().hasCaughtUp()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
        readerFrontend.start();
    }

    private void stopBackgroundThreads() {
        if (nodeDiffHandler != null) {
            nodeDiffHandler.interrupt();
        }
        try {
            readerFrontend.close();
        } catch (IOException e) {
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

    private static class Pair<T,U> {
        public T fst;
        public U snd;

        public static <T,U> Pair of(T fst, U snd) {
            return new Pair<>(fst, snd);
        }

        private Pair(T fst, U snd) {
            this.fst = fst;
            this.snd = snd;
        }
    }

    private static class Router extends Thread implements Closeable {
        private final ZContext context;
        private final String requestBindAddr;
        private final String workerBindAddr;
        private volatile boolean shutDown;
        private ZMQ.Socket requestRouter;
        private ZMQ.Socket workerRouter;
        private final Stack<byte[]> available = new Stack<>();
        private final Map<byte[], byte[]> busyByWorkerId = new HashMap<>();
        private final Map<byte[], byte[]> busyByRequestId = new HashMap<>();
        private ZMQ.Poller poller;
        private Queue<Pair<byte[], byte[]>> pending = new LinkedList<>();

        public Router(ZContext context, String requestBindAddr, String workerBindAddr) {
            super("Backend Router");
            this.context = context;
            this.requestBindAddr = requestBindAddr;
            this.workerBindAddr = workerBindAddr;
        }

        @Override
        public void run() {
            shutDown = false;
            requestRouter = context.createSocket(SocketType.ROUTER);
            requestRouter.bind(requestBindAddr);
            workerRouter = context.createSocket(SocketType.ROUTER);
            poller = context.createPoller(2);
            poller.register(requestRouter, ZMQ.Poller.POLLIN);
            poller.register(workerRouter, ZMQ.Poller.POLLIN);
            workerRouter.bind(workerBindAddr);
            loop: while (!shutDown) {
                try {
                    poller.poll(100);
                    if (poller.pollin(0)) {
                        handleIncomingRequest();
                    } else if (poller.pollin(1)) {
                        handleWorkerRequest();
                    } else {
                        continue loop;
                    }
                    handlePendingRequests();
                } catch (Throwable t) {
                    if (t instanceof InterruptedException) {
                        shutDown = true;
                    } else {
                        log.error(t.getMessage());
                    }
                }
            }
            requestRouter.close();
            workerRouter.close();
            available.clear();
            busyByWorkerId.clear();
            busyByRequestId.clear();
        }

        private void handleIncomingRequest() throws InterruptedException {
            byte[] requestId = requestRouter.recv(); // requester identity
            requestRouter.recvStr();                    // delimiter
            byte[] payload = requestRouter.recv();
            pending.add(Pair.of(requestId, payload));
        }

        private void handlePendingRequests() {
            while (!pending.isEmpty()) {
                Pair<byte[], byte[]> request = pending.peek();
                byte[] workerId = busyByRequestId.get(request.fst);
                if (workerId == null) {
                    if (available.isEmpty()) {
                        return;
                    } else {
                        workerId = available.pop();
                        busyByWorkerId.put(workerId, request.fst);
                        busyByRequestId.put(request.fst, workerId);
                    }
                }
                pending.remove();
                workerRouter.sendMore(workerId);
                workerRouter.sendMore("");
                workerRouter.send(request.snd);
            }
        }

        private void handleWorkerRequest() {
            byte[] workerId = workerRouter.recv();      // worker identity
            workerRouter.recvStr();                     // delimiter
            String verb = workerRouter.recvStr();       // verb (H = Hello, C = Continuation, E = End)
            byte[] payload = workerRouter.recv();
            byte[] requestId = busyByWorkerId.get(workerId);
            if (requestId != null) {
                switch (verb) {
                    case "H":
                        log.error("Got Hello on busy connection");
                        break;
                    case "C":
                        requestRouter.sendMore(requestId);
                        requestRouter.sendMore("");
                        requestRouter.sendMore(verb);
                        requestRouter.send(payload);
                        workerRouter.send(""); // confirm
                        break;
                    case "E":
                        requestRouter.sendMore(requestId);
                        requestRouter.sendMore("");
                        requestRouter.sendMore(verb);
                        requestRouter.send(payload);
                        busyByWorkerId.remove(workerId);
                        busyByRequestId.remove(requestId);
                        available.push(workerId);
                        break;
                    default:
                        log.error("Unknown worker verb {}", verb);
                }
                return;
            }
            if (available.contains(workerId)) {
                log.error("Spurious message from available worker: {}: {}", verb, payload);
                return;
            }
            switch (verb) {
                case "H":
                    log.info("New worker registered: {}", workerId);
                    available.push(workerId);
                    break;
                default:
                    log.error("Expected Hello message from new worker but got {}: {}", verb, payload);
            }
        }

        @Override
        public void close() throws IOException {
            shutDown = true;
        }
    }
}