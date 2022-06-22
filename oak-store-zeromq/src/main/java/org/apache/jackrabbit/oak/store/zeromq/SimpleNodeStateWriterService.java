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
package org.apache.jackrabbit.oak.store.zeromq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimpleNodeStateWriterService implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(SimpleNodeStateWriterService.class);
    private static final String WRITER_REQ_TOPIC = SimpleRequestResponse.Topic.WRITE.toString() + "-req";
    private static final String WRITER_REP_TOPIC = SimpleRequestResponse.Topic.WRITE.toString() + "-rep";
    private static final String WORKER_URL = "inproc://writerBackend";

    private ExecutorService threadPool;
    private Router writerFrontend;
    private SimpleRecordHandler recordHandler;

    private final SimpleBlobStore simpleBlobStore;
    private final String publisherUrl;
    private final String subscriberUrl;

    public SimpleNodeStateWriterService(File blobStoreDir, String publisherUrl, String subscriberUrl) throws IOException {
        this.simpleBlobStore = new SimpleBlobStore(blobStoreDir);
        this.publisherUrl = publisherUrl;
        this.subscriberUrl = subscriberUrl;
    }

    @Override
    public void run() {
        final ZContext context = new ZContext();
        threadPool = Executors.newFixedThreadPool(5);
        final ZMQ.Socket requestSubscriber = context.createSocket(SocketType.SUB);
        requestSubscriber.setBacklog(100000);
        final ZMQ.Socket requestPublisher = context.createSocket(SocketType.PUB);
        requestPublisher.setBacklog(100000);
        requestSubscriber.subscribe(WRITER_REQ_TOPIC);
        requestSubscriber.connect(subscriberUrl);
        requestPublisher.connect(publisherUrl);
        final ZMQ.Socket workerRouter = context.createSocket(SocketType.ROUTER);
        workerRouter.setBacklog(100000);
        workerRouter.bind(WORKER_URL);
        writerFrontend = new Router(requestPublisher, requestSubscriber, workerRouter);
        writerFrontend.start();

        recordHandler = new SimpleRecordHandler(simpleBlobStore, requestPublisher);

        for (int nThread = 0; nThread < 5; ++nThread) {
            threadPool.execute(() -> {
                final ZMQ.Socket socket = context.createSocket(SocketType.REQ);
                socket.setIdentity(("" + Thread.currentThread().getId()).getBytes());
                socket.connect(WORKER_URL);
                socket.setReceiveTimeOut(1000);
                socket.sendMore("H");
                socket.send("");
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        handleWriterService(socket, recordHandler);
                    } catch (Exception e) {
                        System.err.println(e);
                    }
                }
            });
        }

        try {
            while (!Thread.interrupted()) {
                Thread.sleep(1000);
            }
            ;
        } catch (InterruptedException e) {
        }
        threadPool.shutdown();
        try {
            writerFrontend.close();
        } catch (IOException e) {
        }
    }


    public static void handleWriterService(ZMQ.Socket socket, SimpleRecordHandler recordHandler) {
        String msg;
        try {
            msg = socket.recvStr();
        } catch (ZMQException e) {
            socket.sendMore("F");
            socket.send(e.toString());
            return;
        }
        if (msg == null) {
            return; // timeout
        }
        String ret = null;

        try {
            final String uuThreadId = msg;
            ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
            socket.recvByteBuffer(buf, 0);
            buf.rewind();
            final long msgid = buf.getLong();
            final String op = socket.recvStr();
            byte[] value = socket.recv();
            recordHandler.handleRecord(uuThreadId, msgid, op, value);
        } catch (Exception e) {
            final String errorMsg = "An error occurred on: " + msg + ", exception: " + e.getMessage();
            System.err.println(errorMsg);
            socket.sendMore("F");
            socket.send(errorMsg);
            return;
        }
        socket.sendMore("E");
        if (ret == null) {
            ret = "";
        }
        socket.send(ret);
    }

    private static class Router extends Thread implements Closeable {
        private volatile boolean shutDown;
        private final ZMQ.Socket requestSubscriber;
        private final ZMQ.Socket requestPublisher;
        private final ZMQ.Socket workerRouter;
        private final Stack<byte[]> available = new Stack<>();
        private final Map<byte[], byte[]> busyByWorkerId = new HashMap<>();
        private final Map<byte[], byte[]> busyByRequestId = new HashMap<>();
        private Queue<QueuedRequest> pending = new LinkedList<>();

        public Router(ZMQ.Socket requestPublisher, ZMQ.Socket requestSubscriber, ZMQ.Socket workerRouter) {
            super("Backend Router");
            this.requestPublisher = requestPublisher;
            this.requestSubscriber = requestSubscriber;
            this.workerRouter = workerRouter;
        }

        @Override
        public void run() {
            shutDown = false;

            final ZContext context = new ZContext();
            final ZMQ.Poller poller = context.createPoller(2);
            poller.register(requestSubscriber, ZMQ.Poller.POLLIN);
            poller.register(workerRouter, ZMQ.Poller.POLLIN);

            while (!shutDown) {
                try {
                    handlePendingRequests();
                    poller.poll(100);
                    if (poller.pollin(0)) {
                        handleIncomingRequest();
                    } else if (poller.pollin(1)) {
                        handleWorkerRequest();
                    } else {
                        continue;
                    }
                } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                        shutDown = true;
                    }
                    System.err.println(e.getMessage());
                }
            }
            requestPublisher.close();
            requestSubscriber.close();
            workerRouter.close();
            available.clear();
            busyByWorkerId.clear();
            busyByRequestId.clear();
        }

        private void handleIncomingRequest() {
            byte[] requestId = requestSubscriber.recv();
            byte[] msgid = requestSubscriber.recv();
            requestId = Arrays.copyOfRange(requestId, WRITER_REQ_TOPIC.length() + 1, requestId.length);
            byte[] op = requestSubscriber.recv();
            byte[] payload = requestSubscriber.recv();
            pending.add(new QueuedRequest(requestId, msgid, op, payload));
        }

        private void handlePendingRequests() {
            while (!pending.isEmpty()) {
                QueuedRequest request = pending.peek();
                byte[] workerId = busyByRequestId.get(request.clientId);
                if (workerId == null) {
                    if (available.isEmpty()) {
                        return;
                    } else {
                        workerId = available.pop();
                        busyByWorkerId.put(workerId, request.clientId);
                        busyByRequestId.put(request.clientId, workerId);
                    }
                }
                pending.remove();
                workerRouter.sendMore(workerId);
                workerRouter.sendMore("");
                workerRouter.sendMore(request.clientId);
                workerRouter.sendMore(request.msgId);
                workerRouter.sendMore(request.op);
                workerRouter.send(request.payload);
            }
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
                            System.err.println("Got Hello on busy connection");
                            break;
                        case "C":
                        case "E":
                        case "F":
                        case "N":
                            requestPublisher.sendMore(WRITER_REP_TOPIC + " " + new String(requestId));
                            requestPublisher.sendMore(verb);
                            requestPublisher.send(payload);
                            busyByWorkerId.remove(workerId);
                            busyByRequestId.remove(requestId);
                            available.push(workerId);
                            break;
                        default:
                            System.err.println("Unknown worker verb " + verb);
                    }
                    return;
                }
                if (available.contains(workerId)) {
                    System.err.println(String.format("Spurious message from available worker: %1$: %2$", verb, payload));
                    return;
                }
                switch (verb) {
                    case "H":
                        System.err.println("New worker registered: " + new String(workerId));
                        available.push(workerId);
                        break;
                    default:
                        System.err.println(String.format("Expected Hello message from new worker but got %1$: %2$", verb, payload));
                }
            } catch (Throwable t) {
                System.err.println(t.toString());
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

        private static class QueuedRequest {
            public byte[] clientId;
            public byte[] msgId;
            public byte[] op;
            public byte[] payload;

            public QueuedRequest(byte[] clientId, byte[] msgId, byte[] op, byte[] payload) {
                this.clientId = clientId;
                this.msgId = msgId;
                this.op = op;
                this.payload = payload;
            }
        }
    }
}
