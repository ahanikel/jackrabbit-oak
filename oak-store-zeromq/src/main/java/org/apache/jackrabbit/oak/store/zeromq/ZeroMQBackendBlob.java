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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.io.*;
import java.util.*;

@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class ZeroMQBackendBlob implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ZeroMQBackendBlob.class.getName());

    @NotNull
    final ZMQ.Context context;

    @NotNull
    final ZMQ.Poller pollerItems;

    /**
     * read segments to be persisted from this socket
     */
    @NotNull
    final ZMQ.Socket writerService;

    /**
     * the segment reader service serves segments by id
     */
    @NotNull
    final ZMQ.Socket readerService;

    /**
     * our local segment store which keeps the segments
     * we are responsible for
     */
    @NotNull
    final Map<String, ZeroMQBlob> store;

    /**
     * the thread which listens on the sockets and processes messages
     */
    @Nullable
    private final Thread socketHandler;

    private final int clusterInstance;

    private static enum State {
        START, CONT, END;
    }
    private volatile State state = State.START;

    private final Map<String, File> inFlightBlobs = new HashMap<>(50);

    public ZeroMQBackendBlob() {
        clusterInstance = Integer.getInteger("clusterInstance", 0);
        context = ZMQ.context(1);
        readerService = context.socket(ZMQ.REP);
        writerService = context.socket(ZMQ.REP);
        store = new HashMap<>(1000000);
        pollerItems = context.poller(2);
        socketHandler = new Thread("ZeroMQBackendBlob Socket Handler") {
            @Override
            public void run() {
                while (!isInterrupted()) {
                    try {
                        pollerItems.poll();
                        if (pollerItems.pollin(0)) {
                            handleReaderService(readerService.recvStr());
                        }
                        if (pollerItems.pollin(1)) {
                            handleWriterService();
                        }
                    } catch (Throwable t) {
                        log.error(t.toString());
                    }
                }
            }
        };
    }

    public void activate(ComponentContext ctx) {
        open();
    }

    public void deactivate() {
        close();
    }

    void handleReaderService(String msg) {
        synchronized (readerService) {
            final ZeroMQBlob blob = store.getOrDefault(msg, null);
            if (blob != null) {
                sendBlob(blob);
            } else {
                readerService.send("Node not found");
                log.error("Requested node {} not found.", msg);
            }
        }
    }

    void sendBlob(ZeroMQBlob blob) {
        final InputStream is = blob.getNewStream();
        final String reference = blob.getReference();
        String msg;
        final byte[] buffer = new byte[1024 * 1024 * 100]; // 100 MB
        while (true) {
            try {
                synchronized (readerService) {
                    for (int nRead = is.read(buffer); nRead > 0; nRead = is.read(buffer)) {
                        if (nRead < buffer.length) {
                            readerService.sendMore(Arrays.copyOfRange(buffer, 0, nRead));
                        } else {
                            readerService.sendMore(buffer);
                        }
                    }
                    readerService.send("");
                }
                break;
            } catch (Throwable t) {
                log.warn(t.toString());
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    log.error(e.toString());
                }
            }
        }
    }

    InputStream receiveInput() {
        String msg;
        synchronized (writerService) {
            while (true) {
                try {
                    final InputStream is = new InputStream() {
                        final byte[] buffer = writerService.recv();
                        volatile int cur = 0;

                        @Override
                        public int read() {
                            if (cur >= buffer.length) {
                                return -1;
                            }
                            return buffer[cur++];
                        }

                        @Override
                        public int available() {
                            return buffer.length - cur;
                        }
                    };
                    return is;
                } catch (Throwable t) {
                    log.warn("Failed to load blob, retrying: {}", t.getMessage());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                    }
                }
            }
        }
    }

    void handleWriterService() {
        synchronized (writerService) {
            final byte[] bytes = writerService.recv();
            final StringBuffer sbHeader = new StringBuffer();
            try {
                int i;
                for (i = 0; i < bytes.length && bytes[i] != '\n'; ++i) {
                    sbHeader.append((char) bytes[i]);
                }
                if (bytes[i] != '\n') {
                    throw new IllegalStateException("Unable to read header");
                }
                final int sep = sbHeader.indexOf(" ");
                final String headerState = sbHeader.substring(0, sep);
                final String reference = sbHeader.substring(sep + 1);
                switch (headerState) {
                    case "START": {
                        if (inFlightBlobs.containsKey(reference)) {
                            throw new IllegalStateException("Wrong state, expected: CONT, actual: START");
                        }
                        final File out = File.createTempFile("zmqBackendBlob", ".dat");
                        inFlightBlobs.put(reference, out);
                        OutputStream fos = new FileOutputStream(out);
                        fos.write(bytes, ++i, bytes.length - i);
                        fos.flush();
                        fos.close();
                        writerService.send(sbHeader.toString());
                        break;
                    }
                    case "CONT": {
                        File out = inFlightBlobs.get(reference);
                        if (out == null) {
                            throw new IllegalStateException("No blob found to CONT");
                        }
                        OutputStream fos = new FileOutputStream(out, true);
                        fos.write(bytes, ++i, bytes.length - i);
                        fos.flush();
                        fos.close();
                        writerService.send(sbHeader.toString());
                        break;
                    }
                    case "END": {
                        final File out = inFlightBlobs.remove(reference);
                        if (out == null) {
                            throw new IllegalStateException("No blob found to END");
                        }
                        final ZeroMQBlob blob = ZeroMQBlob.newInstance(reference, out);
                        store.putIfAbsent(reference, blob);
                        writerService.send(sbHeader.toString());
                        log.debug("Received our blob {}", reference);
                        break;
                    }
                    default: {
                        throw new IllegalStateException("Unexpected state: " + headerState);
                    }
                }
            } catch (Exception e) {
                log.error(e.toString());
            }
        }
    }

    public void open() {
        readerService.bind("tcp://*:" + (10000 + 2 * clusterInstance));
        writerService.bind("tcp://*:" + (10001 + 2 * clusterInstance));
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

    private void startBackgroundThreads() {
        if (socketHandler != null) {
            socketHandler.start();
        }
    }

    private void stopBackgroundThreads() {
        if (socketHandler != null) {
            socketHandler.interrupt();
        }
    }
}
