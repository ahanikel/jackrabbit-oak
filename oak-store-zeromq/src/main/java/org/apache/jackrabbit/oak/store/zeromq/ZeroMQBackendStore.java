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
import org.zeromq.ZMQException;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public abstract class ZeroMQBackendStore implements BackendStore {

    public static final Logger log = LoggerFactory.getLogger(ZeroMQBackendStore.class);
    public static final String ZEROMQ_READER_URL = "ZEROMQ_READER_URL";
    public static final String ZEROMQ_WRITER_URL = "ZEROMQ_WRITER_URL";
    public static final String ZEROMQ_JOURNAL_URL = "ZEROMQ_JOURNAL_URL";
    public static final String ZEROMQ_NTHREADS   = "ZEROMQ_NTHREADS";
    public static final String ZEROMQ_BACKEND_BLOBCACHE = "ZEROMQ_BACKEND_BLOBCACHE";

    public static abstract class Builder {
        private NodeStateAggregator nodeStateAggregator;
        private String readerUrl;
        private String writerUrl;
        private String journalUrl;
        private int nThreads;
        private String blobCacheDir;

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

        public String getJournalUrl() {
            return journalUrl;
        }

        public Builder withJournalUrl(String journalUrl) {
            this.journalUrl = journalUrl;
            return this;
        }

        public int getNumThreads() {
            return nThreads;
        }

        public Builder withNumThreads(int nThreads) {
            this.nThreads = nThreads;
            return this;
        }

        public String getBlobCacheDir() {
            return blobCacheDir;
        }

        public Builder withBlobCacheDir(String blobCacheDir) {
            this.blobCacheDir = blobCacheDir;
            return this;
        }

        public abstract ZeroMQBackendStore build() throws IOException;

        public Builder initFromEnvironment() {
            readerUrl = System.getenv(ZEROMQ_READER_URL);
            if (readerUrl == null) {
                readerUrl = "tcp://*:8000";
            }
            writerUrl = System.getenv(ZEROMQ_WRITER_URL);
            if (writerUrl == null) {
                writerUrl = "tcp://*:8001";
            }
            journalUrl = System.getenv(ZEROMQ_JOURNAL_URL);
            if (journalUrl == null) {
                journalUrl = "tcp://*:9000";
            }
            try {
                nThreads = Integer.parseInt(System.getenv(ZEROMQ_NTHREADS));
            } catch (NumberFormatException e) {
                nThreads = 4;
            }
            blobCacheDir = System.getenv(ZEROMQ_BACKEND_BLOBCACHE);
            if (blobCacheDir == null) {
                blobCacheDir = "/tmp/backendBlobs";
            }
            return this;
        }
    }

    protected NodeStateAggregator nodeDiffHandler;
    protected Thread nodeDiffHandlerThread;
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
        nodeDiffHandler = builder.getNodeStateAggregator();
        nodeDiffHandlerThread = new Thread(nodeDiffHandler, "ZeroMQBackendStore NodeStateAggregator");

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
                socket.setIdentity(("reader-worker-" + Long.toHexString(Thread.currentThread().getId())).getBytes());
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
        String msg;
        try {
            msg = socket.recvStr();
        }  catch (ZMQException e) {
            log.error(e.toString());
            socket.send("");
            return;
        }
        if (msg == null) {
            log.warn("Timeout occurred on reader socket.");
            return; // timeout
        }
        if ("".equals(msg)) {
            // out-of-line confirmation
            log.warn("Out-of-line confirmation occurred on reader socket.");
            socket.send("");
            return;
        }
        String ret = null;

        try {
            if (msg.startsWith("journal ")) {
                final String instance = msg.substring("journal ".length());
                ret = builder.getNodeStateAggregator().getJournalHead(instance);
            } else if (msg.startsWith("hasblob ")) {
                final Blob blob = builder.getNodeStateAggregator().getBlob(msg.substring("hasblob ".length()));
                ret = blob == null ? "false" : "true";
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
                        do {
                            ret = socket.recvStr();
                            if (ret == null) {
                                log.warn("Timeout occurred on reader socket while waiting for confirmation");
                            }
                        } while (ret == null);
                    }
                } catch (Exception ioe) {
                    log.error(ioe.getMessage());
                } finally {
                    ret = "";
                }
            } else {
                ret = builder.getNodeStateAggregator().readNodeState(msg);
            }
        } catch (Exception e) {
            if (ret == null) {
                log.error("Requested node not found: {}, exception: {}", msg, e.getMessage());
            } else {
                log.error(e.toString());
            }
        } finally {
            socket.sendMore("E");
            if (ret == null) {
                ret = "";
            }
            socket.send(ret);
        }
    }

    @Override
    public void handleWriterService(ZMQ.Socket socket) {
        final String msg = socket.recvStr();
        eventWriter.accept(msg);
        socket.send("confirmed");
    }

    private void startBackgroundThreads() {
        if (nodeDiffHandlerThread != null) {
            nodeDiffHandlerThread.start();
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
        if (nodeDiffHandlerThread != null) {
            try {
                nodeDiffHandler.close();
                nodeDiffHandlerThread.join();
            } catch (InterruptedException | IOException e) {
            }
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
        writerFrontend.close();
        writerBackend.close();
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
        private final Map<byte[], byte[]> busyByWorkerId = new ConcurrentHashMap<>();
        private final Map<byte[], byte[]> busyByRequestId = new ConcurrentHashMap<>();
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
            requestRouter.setBacklog(100000);
            workerRouter = context.createSocket(SocketType.ROUTER);
            workerRouter.setBacklog(100000);
            poller = context.createPoller(2);
            poller.register(requestRouter, ZMQ.Poller.POLLIN);
            poller.register(workerRouter, ZMQ.Poller.POLLIN);
            requestRouter.bind(requestBindAddr);
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
            byte[] workerId = busyByRequestId.get(requestId);
            if (workerId == null) {
                synchronized (available) {
                    if (available.isEmpty()) {
                        available.wait();
                    }
                    workerId = available.pop();
                    busyByWorkerId.put(workerId, requestId);
                    busyByRequestId.put(requestId, workerId);
                }
            }
            workerRouter.sendMore(workerId);
            workerRouter.sendMore("");
            workerRouter.send(payload);
        }

        private void handleWorkerRequest() {
            try {
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
                            break;
                        case "E":
                            requestRouter.sendMore(requestId);
                            requestRouter.sendMore("");
                            requestRouter.sendMore(verb);
                            requestRouter.send(payload);
                            busyByWorkerId.remove(workerId);
                            busyByRequestId.remove(requestId);
                            synchronized (available) {
                                available.push(workerId);
                                available.notify();
                            }
                            break;
                        default:
                            log.error("Unknown worker verb {}", verb);
                    }
                    return;
                }
                synchronized (available) {
                    if (available.contains(workerId)) {
                        log.error("Spurious message from available worker: {}: {}", verb, payload);
                        return;
                    }
                }
                switch (verb) {
                    case "H":
                        log.info("New worker registered: {}", workerId);
                        synchronized (available) {
                            available.push(workerId);
                            available.notify();
                        }
                        break;
                    default:
                        log.error("Expected Hello message from new worker but got {}: {}", verb, payload);
                }
            } catch (Throwable t) {
                log.error(t.toString());
                throw t;
            }
        }

        @Override
        public void close() throws IOException {
            shutDown = true;
            try {
                this.join();
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
    }
}
