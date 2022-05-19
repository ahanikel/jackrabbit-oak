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
package org.apache.jackrabbit.oak.simple;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimpleNodeStateWriterServiceCommand implements Command {

    public static final String NAME = "simple-nodestate-writer-service";

    private static final String summary = "Serves the contents of a simple blobstore via ZeroMQ REP\n" +
        "Example:\n" + NAME + " /tmp/imported/log.txt";

    private static final String WRITER_TOPIC = "write";
    private static final String JOURNAL_TOPIC = "journal";

    private File blobDir;
    private ZContext context;
    private ExecutorService threadPool;
    private Router writerFrontend;
    private SimpleRecordHandler recordHandler;

    @Override
    public void execute(String... args) throws Exception {
        final OptionParser parser = new OptionParser();

        final Options opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(summary);

        final OptionSet optionSet = opts.parseAndConfigure(parser, args);
        final List<?> uris = optionSet.nonOptionArguments();

        if (uris.size() != 1) {
            throw new IllegalArgumentException(summary);
        }

        final CommonOptions commonOptions = opts.getOptionBean(CommonOptions.class);
        final URI uri = commonOptions.getURI(0);
        blobDir = new File(uri.getPath());
        context = new ZContext();
        threadPool = Executors.newFixedThreadPool(5);
        writerFrontend = new Router(context, "tcp://comm-hub:8000", "tcp://comm-hub:8001", "inproc://writerBackend");
        writerFrontend.start();

        recordHandler = new SimpleRecordHandler(blobDir, "tcp://*:9000");

        for (int nThread = 0; nThread < 5; ++nThread) {
            threadPool.execute(() -> {
                final ZMQ.Socket socket = context.createSocket(SocketType.REQ);
                socket.setIdentity(("" + Thread.currentThread().getId()).getBytes());
                socket.connect("inproc://writerBackend");
                socket.setReceiveTimeOut(1000);
                socket.sendMore("H");
                socket.send("");
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        handleWriterService(socket);
                    } catch (Throwable t) {
                        System.err.println(t.toString());
                    }
                }
            });
        }

        while (!Thread.interrupted()) {
            Thread.sleep(1000);
        };
        threadPool.shutdown();
        writerFrontend.close();
    }

    public void handleWriterService(ZMQ.Socket socket) {
        String msg;
        try {
            msg = socket.recvStr();
        } catch (ZMQException e) {
            socket.send("");
            return;
        }
        if (msg == null) {
            return; // timeout
        }
        String ret = null;

        try {
            final StringTokenizer st = new StringTokenizer(msg);
            final String uuThreadId = st.nextToken();
            final String op = st.nextToken();
            String value;
            try {
                value = st.nextToken("").substring(1);
            } catch (NoSuchElementException e) {
                value = "";
            }
            recordHandler.handleRecord(uuThreadId, op, value);
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
        private Queue<Pair<byte[], byte[]>> pending = new LinkedList<>();

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
            requestSubscriber.subscribe(WRITER_TOPIC);
            requestSubscriber.connect(requestSubConnectAddr);
            requestPublisher.connect(requestPubConnectAddr);
            workerRouter.bind(workerBindAddr);
            loop:
            while (!shutDown) {
                try {
                    handlePendingRequests();
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
                        System.err.println(t.getMessage());
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

        private void handleIncomingRequest() throws InterruptedException {
            byte[] request = requestSubscriber.recv();
            int startRequestId = WRITER_TOPIC.length() + 1;
            int endRequestId;
            for (endRequestId = startRequestId; endRequestId < request.length && request[endRequestId] != ' '; ++endRequestId);
            byte[] requestId = Arrays.copyOfRange(request, startRequestId, endRequestId);
            byte[] payload = Arrays.copyOfRange(request, endRequestId + 1, request.length);
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
                            requestPublisher.sendMore(verb);
                            requestPublisher.send(WRITER_TOPIC + " " + new String(requestId) + " " + new String(payload)); // TODO: inefficient
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
    }
}