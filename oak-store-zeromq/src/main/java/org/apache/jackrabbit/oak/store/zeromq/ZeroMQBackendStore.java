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
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
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

    private int readerPort;

    private int writerPort;

    private int nThreads;

    private final Executor threadPool;

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
        threadPool = Executors.newFixedThreadPool(2 * nThreads + 2);
        for (int nThread = 0; nThread < 1; ++nThread) {
            threadPool.execute(() -> {
                ServerSocket socket = null;
                do {
                    try {
                        socket = new ServerSocket(readerPort);
                    } catch (IOException e) {
                        log.error(e.getMessage());
                    }
                } while (socket == null);
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Socket conn = socket.accept();
                        threadPool.execute(() -> {
                            try {
                                handleReaderService(conn);
                            } catch (IOException e) {
                                log.error("Socket error: ", e);
                            }
                        });
                    } catch (Throwable t) {
                        log.error(t.toString());
                    }
                }
            });
        };
        for (int nThread = 0; nThread < 1; ++nThread) {
            threadPool.execute(() -> {
                ServerSocket socket = null;
                do {
                    try {
                        socket = new ServerSocket(writerPort);
                    } catch (IOException ioe) {
                        log.error(ioe.getMessage());
                    }
                } while (socket == null);
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Socket conn = socket.accept();
                        threadPool.execute(() -> {
                            try {
                                handleWriterService(conn);
                            } catch (IOException e) {
                                log.error("Socket error", e);
                            }
                        });
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
    public void handleReaderService(Socket socket) throws IOException {
        final String msg = IOUtils.readString(socket.getInputStream());
        String ret = null;
        if (msg.startsWith("journal ")) {
            final String instance = msg.substring("journal ".length());
            ret = nodeStateAggregator.getJournalHead(instance);
        } else if (msg.startsWith("blob ")) {
            byte[] buffer = new byte[1024 * 1024]; // not final because of fear it's not being GC'd
            try {
                final Blob blob = nodeStateAggregator.getBlob(msg.substring("blob ".length()));
                if (blob == null) {
                    throw new IllegalArgumentException(msg + " not found");
                }
                final InputStream is = blob.getNewStream();
                final OutputStream os = socket.getOutputStream();
                IOUtils.copy(is, os);
                is.close();
                os.close();
                socket.close();
            } catch (IllegalArgumentException iae) {
                log.trace(iae.getMessage());
            } catch (Exception ioe) {
                log.error(ioe.getMessage());
            }
            return;
        } else {
            ret = nodeStateAggregator.readNodeState(msg);
        }
        if (ret != null) {
            IOUtils.writeString(socket.getOutputStream(), ret);
        } else {
            IOUtils.writeString(socket.getOutputStream(), "Node not found");
            log.error("Requested node not found: {}", msg);
        }
        socket.close();
    }

    @Override
    public void handleWriterService(Socket socket) throws IOException {
        final String msg = IOUtils.readString(socket.getInputStream());
        eventWriter.accept(msg);
        IOUtils.writeString(socket.getOutputStream(), "confirmed");
        socket.close();
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
    }
}
