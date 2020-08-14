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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.io.Closeable;
import java.io.InputStream;
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
        final ZeroMQBlob blob = store.getOrDefault(msg, null);
        if (blob != null) {
            sendBlob(blob);
        } else {
            readerService.send("Node not found");
            log.error("Requested node {} not found.", msg);
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
                    readerService.sendMore(reference + "\n");
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

    InputStream receiveBlob() {
        String msg;
        synchronized (writerService) {
            while (true) {
                try {
                    final InputStream is = new InputStream() {
                        final byte[] buffer = new byte[1024 * 1024 * 100]; // 100 MB
                        volatile int cur = 0;
                        volatile int max = writerService.recv(buffer, 0, buffer.length, 0);

                        @Override
                        public int read() {
                            if (cur == max) {
                                nextBunch();
                            }
                            if (max < 1) {
                                return -1;
                            }
                            return buffer[cur++];
                        }

                        private void nextBunch() {
                            synchronized (writerService) {
                                max = writerService.recv(buffer, 0, buffer.length, 0);
                                cur = 0;
                            }
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
        final InputStream is = receiveBlob();
        final StringBuffer sbReference = new StringBuffer();
        try {
            int c;
            for (c = is.read(); c >= 0 && c != '\n'; c = is.read()) {
                sbReference.append((char) c);
            }
            if (c != '\n') {
                throw new IllegalStateException("Unable to read blob reference");
            }
            final String reference = sbReference.toString();
            final ZeroMQBlob blob = ZeroMQBlob.newInstance(is);
            store.putIfAbsent(reference, blob);
            writerService.send(reference + " confirmed.");
            log.debug("Received our blob {}", reference);
        } catch (Exception e) {
            log.error(e.toString());
        }
    }

    public void open() {
        readerService.bind("tcp://*:" + (8000 + 2 * clusterInstance));
        writerService.bind("tcp://*:" + (8001 + 2 * clusterInstance));
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
