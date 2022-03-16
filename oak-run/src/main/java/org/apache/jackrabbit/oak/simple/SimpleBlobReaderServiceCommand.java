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
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.store.zeromq.SimpleBlobStore;
import org.apache.jackrabbit.oak.store.zeromq.ZeroMQEmptyNodeState;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimpleBlobReaderServiceCommand implements Command {

    public static final String NAME = "simple-blob-reader-service";

    private static final String summary = "Serves the contents of a simple blobstore via ZeroMQ REP\n" +
        "Example:\n" +
        "simple-blob-reader-service simple:///tmp/imported";

    private File blobDir;
    private SimpleBlobStore blobStore;
    private ZContext context;
    private ExecutorService threadPool;
    private Router readerFrontend;

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
        blobStore = new SimpleBlobStore(blobDir);
        context = new ZContext();
        threadPool = Executors.newFixedThreadPool(5);
        readerFrontend = new Router(context, "tcp://*:8000", "inproc://readerBackend");
        readerFrontend.start();

        for (int nThread = 0; nThread < 5; ++nThread) {
            threadPool.execute(() -> {
                final ZMQ.Socket socket = context.createSocket(SocketType.REQ);
                socket.setIdentity(("" + Thread.currentThread().getId()).getBytes());
                socket.connect("inproc://readerBackend");
                socket.setReceiveTimeOut(1000);
                socket.sendMore("H");
                socket.send("");
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        handleReaderService(socket);
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
        readerFrontend.close();
    }

    public void handleReaderService(ZMQ.Socket socket) {
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
            if (msg.startsWith("journal ")) {
                final String instance = msg.substring("journal ".length());
                ret = getJournalHead(instance);
            } else if (msg.startsWith("hasblob ")) {
                final String ref = msg.substring("hasblob ".length());
                ret = blobStore.hasBlob(ref) ? "true" : "false";
            } else if (msg.startsWith("blob ")) {
                FileInputStream fis = null;
                try {
                    final StringTokenizer st = new StringTokenizer(msg);
                    st.nextToken();
                    final String reference = st.nextToken();
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
                            socket.sendMore("E");
                        } else {
                            socket.sendMore("C");
                        }
                        socket.sendByteBuffer(buffer, 0);
                    } else {
                        socket.sendMore("F");
                        socket.send("Unknown command");
                    }
                } catch (FileNotFoundException fnf) {
                    socket.sendMore("N");
                    socket.send("");
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                    socket.sendMore("F");
                    socket.send("" + e.getMessage());
                } finally {
                    if (fis != null) {
                        fis.close();
                    }
                }
                return;
            }
        } catch (Exception e) {
            final String errorMsg = String.format("An error occurred on: {}, exception: {}", msg, e.getMessage());
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

    private String getJournalHead(String journalName) throws IOException {
        final File journalFile = blobStore.getSpecificFile("journal-" + journalName);
        try (FileInputStream is = new FileInputStream(journalFile)) {
            return IOUtils.readString(is);
        } catch (FileNotFoundException e) {
            return ZeroMQEmptyNodeState.UUID_NULL.toString();
        }
    }

    private static int parseIntWithDefault(StringTokenizer st, int def) {
        try {
            return Integer.parseInt(st.nextToken());
        } catch (Exception e) {
            return def;
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
            requestRouter.setBacklog(100000);
            workerRouter = context.createSocket(SocketType.ROUTER);
            workerRouter.setBacklog(100000);
            poller = context.createPoller(2);
            poller.register(requestRouter, ZMQ.Poller.POLLIN);
            poller.register(workerRouter, ZMQ.Poller.POLLIN);
            requestRouter.bind(requestBindAddr);
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
                            requestRouter.sendMore(requestId);
                            requestRouter.sendMore("");
                            requestRouter.sendMore(verb);
                            requestRouter.send(payload);
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
                    System.err.println(String.format("Spurious message from available worker: {}: {}", verb, payload));
                    return;
                }
                switch (verb) {
                    case "H":
                        System.err.println("New worker registered: " + new String(workerId));
                        available.push(workerId);
                        break;
                    default:
                        System.err.println(String.format("Expected Hello message from new worker but got {}: {}", verb, payload));
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