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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A store used for in-memory operations.
 */
@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class ZeroMQBackendStore implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ZeroMQBackendStore.class.getName());

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
    final Map<String, String> store;

    /**
     * the thread which listens on the sockets and processes messages
     */
    @Nullable
    private final Thread socketHandler;

    private final int clusterInstance;

    public ZeroMQBackendStore() {
        clusterInstance = Integer.getInteger("clusterInstance", 0);
        context = ZMQ.context(1);
        readerService = context.socket(ZMQ.REP);
        writerService = context.socket(ZMQ.REP);
        store = new HashMap<>();
        ZeroMQNodeState ns = (ZeroMQNodeState) ZeroMQEmptyNodeState.EMPTY_NODE(null, null, null);
        final List<ZeroMQNodeState.SerialisedZeroMQNodeState> sNs = new ArrayList<>();
        ns.serialise(sNs::add);
        store.put(ns.getUuid(), sNs.get(0).getserialisedNodeState());
        pollerItems = context.poller(2);
        socketHandler = new Thread("ZeroMQBackendStore Socket Handler") {
            @Override
            public void run() {
                while (!isInterrupted()) {
                    try {
                        pollerItems.poll();
                        if (pollerItems.pollin(0)) {
                            handleReaderService(readerService.recvStr());
                        }
                        if (pollerItems.pollin(1)) {
                            handleWriterService(writerService.recvStr());
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

    void handleReaderService(String msg) {
        final String sNode = store.getOrDefault(msg, null);
        if (sNode != null) {
            readerService.send(sNode);
        } else {
            readerService.send("Node not found");
            log.error("Requested node {} not found.", msg);
        }
    }

    void handleWriterService(String msg) {
        try {
            final String uuid = ZeroMQNodeState.parseUuidFromSerialisedNodeState(msg);
            store.put(uuid, msg);
            writerService.send(uuid + " confirmed.");
            log.debug("Received our node {}", uuid);
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
    public void close() throws IOException {
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
