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
package org.apache.jackrabbit.oak.segment.memory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.Closeable;
import java.io.IOException;
import org.apache.jackrabbit.oak.segment.spi.persistence.Buffer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 * A store used for in-memory operations.
 */
@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class ZeroMQStore implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ZeroMQStore.class.getName());

    static final int UUID_LEN = 36;

    @NotNull
    final ZMQ.Context context;

    @NotNull
    final ZMQ.Poller pollerItems;

    /**
     * read segments to be persisted from this socket
     */
    @NotNull
    final ZMQ.Socket segmentWriterService;

    /**
     * the segment reader service serves segments by id
     */
    @NotNull
    final ZMQ.Socket segmentReaderService;

    /**
     * our local segment store which keeps the segments
     * we are responsible for
     */
    @NotNull
    final Cache<String, byte[]> segmentStore;

    /**
     * the thread which listens on the sockets and processes messages
     */
    @Nullable
    private final Thread socketHandler;

    private final int clusterInstance;

    public ZeroMQStore() throws IOException {
        clusterInstance = Integer.getInteger("clusterInstance") - 1;
        context = ZMQ.context(1);
        segmentWriterService = context.socket(ZMQ.REP);
        segmentReaderService = context.socket(ZMQ.REP);
        segmentStore = CacheBuilder.newBuilder().build();
        pollerItems = context.poller(2);
        socketHandler = new Thread("ZeroMQStore Socket Handler") {
            @Override
            public void run() {
                while (!isInterrupted()) {
                    try {
                        pollerItems.poll();
                        if (pollerItems.pollin(0)) {
                            handleSegmentReaderService(segmentReaderService.recv(0));
                        }
                        if (pollerItems.pollin(1)) {
                            handleSegmentWriterService(segmentWriterService.recv(0));
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
        try {
            close();
        } catch (IOException ioe) {
            log.warn(ioe.toString());
        }
    }

    void handleSegmentReaderService(byte[] msg) {
        final String id = new String(msg, 0, UUID_LEN);
        final byte[] segmentBytes = segmentStore.getIfPresent(id);
        if (segmentBytes != null) {
            segmentReaderService.send(segmentBytes);
        } else {
            segmentReaderService.send("Segment not found");
            log.error("Requested segment {} not found.", id);
        }
    }

    void handleSegmentWriterService(byte[] msg) {
        final String id = new String(msg, 0, UUID_LEN);
        final Buffer segmentBytes = Buffer.allocate(msg.length - UUID_LEN);
        segmentBytes.put(msg, UUID_LEN, msg.length - UUID_LEN);
        segmentBytes.rewind();
        segmentStore.put(id, segmentBytes.array());
        segmentWriterService.send(id + " confirmed.");
        log.debug("Received our segment {}", id);
    }

    public void open() {
        segmentWriterService.bind("tcp://localhost:" + (8000 + 2 * clusterInstance));
        segmentReaderService.bind("tcp://localhost:" + (8001 + 2 * clusterInstance));
        pollerItems.register(segmentReaderService, ZMQ.Poller.POLLIN);
        pollerItems.register(segmentWriterService, ZMQ.Poller.POLLIN);
        startBackgroundThreads();
    }

    @Override
    public void close() throws IOException {
        stopBackgroundThreads();
        pollerItems.close();
        segmentWriterService.close();
        segmentReaderService.close();
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
