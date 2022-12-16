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

import org.apache.jackrabbit.oak.commons.IOUtils;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimpleBlobReaderService implements Runnable {

    private static final String READER_REQ_TOPIC = SimpleRequestResponse.Topic.READ.toString() + "-req";
    private static final String READER_REP_TOPIC = SimpleRequestResponse.Topic.READ.toString() + "-rep";

    private SimpleBlobStore blobStore;
    private final String publisherUrl;
    private final String subscriberUrl;
    private ZContext context;
    private ExecutorService threadPool;
    private Router readerFrontend;

    public SimpleBlobReaderService(File blobDir, String publisherUrl, String subscriberUrl) throws IOException {
        this.blobStore = new SimpleBlobStore(blobDir);
        this.publisherUrl = publisherUrl;
        this.subscriberUrl = subscriberUrl;
    }

    @Override
    public void run() {
        context = new ZContext();
        threadPool = Executors.newFixedThreadPool(5);
        readerFrontend = new Router(context, subscriberUrl, publisherUrl, "inproc://readerBackend");
        readerFrontend.start();

        for (int nThread = 0; nThread < 5; ++nThread) {
            threadPool.execute(() -> {
                final ZMQ.Socket socket = context.createSocket(SocketType.REQ);
                socket.setIdentity(("" + Thread.currentThread().getId()).getBytes());
                socket.connect("inproc://readerBackend");
                socket.setReceiveTimeOut(1000);
                socket.sendMore(Util.LONG_ZERO);
                socket.sendMore(Util.LONG_ZERO);
                socket.sendMore("H");
                socket.send("");
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        handleReaderService(socket, blobStore);
                    } catch (Throwable t) {
                        System.err.println(t.toString());
                    }
                }
            });
        }

        try {
            while (!Thread.interrupted()) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
        }
        threadPool.shutdown();
        try {
            readerFrontend.close();
        } catch (Exception e) {
        }
    }

    public static void handleReaderService(ZMQ.Socket socket, BlobStore blobStore) {
        byte[] msgId;
        String msg;
        try {
            msgId = socket.recv();
        } catch (ZMQException e) {
            socket.sendMore("F");
            socket.send(e.getMessage());
            return;
        }
        if (msgId == null) {
            return; // timeout
        }

        String ret = null;
        msg = socket.recvStr();

        try {
            if (msg.equals("journal")) {
                final String instance = socket.recvStr();
                if (instance.contains("/")) {
                    throw new IllegalArgumentException();
                }
                ret = getJournalHead(instance, blobStore);
            } else if (msg.equals("hasblob")) {
                final String ref = socket.recvStr();
                if (ref.contains("/")) {
                    throw new IllegalArgumentException();
                }
                ret = blobStore.hasBlob(ref) ? "true" : "false";
            } else if (msg.equals("blob")) {
                FileInputStream fis = null;
                try {
                    msg = socket.recvStr();
                    final StringTokenizer st = new StringTokenizer(msg);
                    final String reference = st.nextToken();
                    if (reference.contains("/")) {
                        throw new IllegalArgumentException();
                    }
                    final int offset = parseIntWithDefault(st, 0);
                    final int maxSize = parseIntWithDefault(st, -1);
                    final ByteBuffer buffer = ByteBuffer.allocate(
                            maxSize <= 0 || maxSize > 1048576 ? 1048576 : maxSize);
                    fis = blobStore.getInputStream(reference);
                    int nRead = fis.getChannel().read(buffer, offset);
                    if (nRead > 0) {
                        buffer.limit(nRead);
                        buffer.rewind();
                        if (offset + nRead == fis.getChannel().size()) {
                            socket.sendMore(msgId);
                            socket.sendMore(Util.LONG_ZERO);
                            socket.sendMore("E");
                        } else {
                            socket.sendMore(msgId);
                            socket.sendMore(Util.LONG_ZERO);
                            socket.sendMore("C");
                        }
                        socket.sendByteBuffer(buffer, 0);
                    } else {
                        socket.sendMore(msgId);
                        socket.sendMore(Util.LONG_ZERO);
                        socket.sendMore("F");
                        socket.send("Unknown command");
                    }
                } catch (FileNotFoundException fnf) {
                    socket.sendMore(msgId);
                    socket.sendMore(Util.LONG_ZERO);
                    socket.sendMore("N");
                    socket.send("");
                } catch (IllegalArgumentException e) {
                    socket.sendMore(msgId);
                    socket.sendMore(Util.LONG_ZERO);
                    socket.sendMore("F");
                    socket.send("IllegalArgument");
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                    socket.sendMore(msgId);
                    socket.sendMore(Util.LONG_ZERO);
                    socket.sendMore("F");
                    socket.send("" + e.getMessage());
                } finally {
                    if (fis != null) {
                        fis.close();
                    }
                }
                return;
            }
        } catch (IllegalArgumentException e) {
            socket.sendMore(msgId);
            socket.sendMore(Util.LONG_ZERO);
            socket.sendMore("F");
            socket.send("IllegalArgument");
            return;
        } catch (Exception e) {
            final String errorMsg = String.format("An error occurred: %s", e.toString());
            System.err.println(errorMsg);
            socket.sendMore(msgId);
            socket.sendMore(Util.LONG_ZERO);
            socket.sendMore("F");
            socket.send(errorMsg);
            return;
        }
        socket.sendMore(msgId);
        socket.sendMore(Util.LONG_ZERO);
        socket.sendMore("E");
        if (ret == null) {
            ret = "";
        }
        socket.send(ret);
    }

    private static String getJournalHead(String journalName, BlobStore blobStore) throws IOException {
        final File journalFile = blobStore.getSpecificFile("journal-" + journalName);
        try (FileInputStream is = new FileInputStream(journalFile)) {
            return IOUtils.readString(is);
        } catch (FileNotFoundException e) {
            return SimpleNodeState.UUID_NULL.toString();
        }
    }

    private static int parseIntWithDefault(StringTokenizer st, int def) {
        try {
            return Integer.parseInt(st.nextToken());
        } catch (Exception e) {
            return def;
        }
    }

    // TODO: extract this
    private static class Router extends Thread implements Closeable {
        private final ZContext context;
        private final String requestSubConnectAddr;
        private final String requestPubConnectAddr;
        private final String workerBindAddr;
        private volatile boolean shutDown;
        private ZMQ.Socket requestSubscriber;
        private ZMQ.Socket requestPublisher;
        private ZMQ.Socket workerRouter;
        private final Stack<byte[]> available = new Stack<>();
        private final Map<byte[], byte[]> busyByWorkerId = new HashMap<>();
        private final Map<byte[], byte[]> busyByRequestId = new HashMap<>();
        private ZMQ.Poller poller;
        private Queue<QueuedRequest> pending = new LinkedList<>();

        public Router(ZContext context, String requestSubConnectAddr, String requestPubConnectAddr, String workerBindAddr) {
            super("Backend Router");
            this.context = context;
            this.requestSubConnectAddr = requestSubConnectAddr;
            this.requestPubConnectAddr = requestPubConnectAddr;
            this.workerBindAddr = workerBindAddr;
        }

        @Override
        public void run() {
            shutDown = false;
            requestSubscriber = context.createSocket(SocketType.SUB);
            requestSubscriber.setBacklog(100000);
            requestPublisher = context.createSocket(SocketType.PUB);
            requestPublisher.setBacklog(100000);
            workerRouter = context.createSocket(SocketType.ROUTER);
            workerRouter.setBacklog(100000);
            poller = context.createPoller(2);
            poller.register(requestSubscriber, ZMQ.Poller.POLLIN);
            poller.register(workerRouter, ZMQ.Poller.POLLIN);
            requestSubscriber.subscribe(READER_REQ_TOPIC);
            requestSubscriber.connect(requestSubConnectAddr);
            requestPublisher.connect(requestPubConnectAddr);
            workerRouter.bind(workerBindAddr);

            while (!shutDown) {
                try {
                    handlePendingRequests();
                    poller.poll(100);
                    if (poller.pollin(1)) {
                        handleWorkerRequest(); // worker request comes first, otherwise queue is never worked on
                    } else if (poller.pollin(0)) {
                        handleIncomingRequest();
                    } else {
                        continue;
                    }
                } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                        shutDown = true;
                    }
                    System.err.println(e.toString());
                    while (requestSubscriber.hasReceiveMore()) {
                        requestSubscriber.recv(); // throw away the remaining message parts, if any
                    }
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
            byte[] msgId = requestSubscriber.recv(); // msgid, ignored for now because read requests are idempotent anyway
            requestId = Arrays.copyOfRange(requestId, READER_REQ_TOPIC.length() + 1, requestId.length);
            byte[] op = requestSubscriber.recv();
            byte[] payload = requestSubscriber.recv();
            pending.add(new QueuedRequest(requestId, msgId, op, payload));
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
                workerRouter.sendMore(request.msgId);
                workerRouter.sendMore(request.op);
                workerRouter.send(request.payload);
            }
        }

        private void handleWorkerRequest() {
            byte[] workerId = workerRouter.recv();      // worker identity
            workerRouter.recvStr();                     // delimiter
            byte[] reqMsgId = workerRouter.recv();
            byte[] repMsgId = workerRouter.recv();
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
                        requestPublisher.sendMore(READER_REP_TOPIC + " " + new String(requestId));
                        requestPublisher.sendMore(reqMsgId);
                        requestPublisher.sendMore(repMsgId);
                        requestPublisher.sendMore(verb);
                        requestPublisher.send(payload);
                        busyByWorkerId.remove(workerId);
                        busyByRequestId.remove(requestId);
                        available.push(workerId);
                        break;
                    default:
                        System.err.println("Unknown worker verb " + verb);
                }
            } else if (available.contains(workerId)) {
                System.err.println(String.format("Spurious message from available worker: {}: {}", verb, payload));
            } else {
                switch (verb) {
                    case "H":
                        System.err.println("New worker registered: " + new String(workerId));
                        available.push(workerId);
                        break;
                    default:
                        System.err.println(String.format("Expected Hello message from new worker but got {}: {}", verb, payload));
                }
            }
            while (workerRouter.hasReceiveMore()) {
                System.err.println(String.format("Throwing away spurious message part: {}", workerRouter.recvStr()));
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
