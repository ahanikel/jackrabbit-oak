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

import java.io.*;
import java.util.Arrays;

@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class ZeroMQBackendBlob implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ZeroMQBackendBlob.class.getName());

    @NotNull
    final ZMQ.Context context;

    @NotNull
    final ZMQ.Poller pollerItems;

    /**
     * read blobs to be persisted from this socket
     */
    @NotNull
    final ZMQ.Socket writerService;

    /**
     * the blob reader service serves blobs by id
     */
    @NotNull
    final ZMQ.Socket readerService;

    /**
     * the thread which listens on the sockets and processes messages
     */
    @Nullable
    private Thread socketHandler;

    private final int clusterInstance;

    private static final File blobCache = new File("/tmp/backendblobs");

    static {
        blobCache.mkdir();
    }

    public ZeroMQBackendBlob() {
        clusterInstance = Integer.getInteger("clusterInstance", 0);
        context = ZMQ.context(1);
        readerService = context.socket(ZMQ.REP);
        writerService = context.socket(ZMQ.REP);
        pollerItems = context.poller(2);

    }

    public void activate(ComponentContext ctx) {
        open();
    }

    public void deactivate() {
        close();
    }

    void handleReaderService(String msg) {
        synchronized (readerService) {
            try {
                final File blob = new File(blobCache, msg);
                if (blob.exists()) {
                    sendBlob(msg, blob);
                } else {
                    log.error("Requested node {} not found.", msg);
                }
            } catch (Throwable t) {
                log.error("An error occurred when trying to send blob {}: {}", msg, t.toString());
            } finally {
                readerService.send(new byte[0]);
            }
        }
    }

    void sendBlob(String reference, File blob) throws FileNotFoundException {
        final InputStream is = new FileInputStream(blob);
        final byte[] buffer = new byte[1024 * 1024]; // 100 MB
        try {
            synchronized (readerService) {
                for (int nRead = is.read(buffer); nRead > 0; nRead = is.read(buffer)) {
                    if (nRead < buffer.length) {
                        readerService.sendMore(Arrays.copyOfRange(buffer, 0, nRead));
                    } else {
                        readerService.sendMore(buffer);
                    }
                }
            }
        } catch (Throwable t) {
            log.error(t.toString());
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                log.error(e.toString());
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
            try {
                final String reference = writerService.recvStr();
                final File out = File.createTempFile("zmqBackendBlob", ".dat");
                OutputStream fos = new FileOutputStream(out);
                while (writerService.hasReceiveMore()) {
                    fos.write(writerService.recv());
                }
                fos.flush();
                fos.close();
                writerService.send(reference);
                final File destFile = new File(blobCache, reference);
                if (destFile.exists()) {
                    out.delete();
                } else {
                    out.renameTo(destFile);
                }
                log.debug("Received our blob {}", reference);
            } catch (Exception e) {
                log.error(e.toString());
            }
        }
    }

    public void open() {
        readerService.bind("tcp://*:" + (11000 + 2 * clusterInstance));
        writerService.bind("tcp://*:" + (11001 + 2 * clusterInstance));
        pollerItems.register(readerService, ZMQ.Poller.POLLIN);
        pollerItems.register(writerService, ZMQ.Poller.POLLIN);
        startBackgroundThreads();
    }

    @Override
    public void close() {
        stopBackgroundThreads();
        pollerItems.unregister(writerService);
        pollerItems.unregister(readerService);
        readerService.unbind("tcp://*:" + (11000 + 2 * clusterInstance));
        writerService.unbind("tcp://*:" + (11001 + 2 * clusterInstance));
    }

    private void startBackgroundThreads() {
        if (socketHandler != null) {
            return;
        }
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
        socketHandler.start();
    }

    private void stopBackgroundThreads() {
        if (socketHandler != null) {
            socketHandler.interrupt();
            socketHandler = null;
        }
    }
}
