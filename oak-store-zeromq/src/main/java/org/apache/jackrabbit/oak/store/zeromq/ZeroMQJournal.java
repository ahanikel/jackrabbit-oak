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
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.io.Closeable;
import java.io.IOException;

/**
 * A journal for the cloud
 */
@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class ZeroMQJournal implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ZeroMQJournal.class.getName());

    @NotNull
    final ZMQ.Context context;

    @NotNull
    final ZMQ.Poller pollerItems;

    /**
     * read record ids to be persisted from this socket
     */
    @NotNull
    final ZMQ.Socket journalWriterService;

    /**
     * the journal reader service serves the current head record id
     */
    @NotNull
    final ZMQ.Socket journalReaderService;

    /**
     * the thread which listens on the sockets and processes messages
     */
    @Nullable
    private final Thread socketHandler;

    @NotNull
    private byte[] head = "undefined".getBytes();

    public ZeroMQJournal() {

        context = ZMQ.context(1);

        journalReaderService = context.socket(ZMQ.REP);
        journalReaderService.bind("tcp://*:9000");

        journalWriterService = context.socket(ZMQ.REP);
        journalWriterService.bind("tcp://*:9001");

        pollerItems = context.poller(2);
        pollerItems.register(journalReaderService, ZMQ.Poller.POLLIN);
        pollerItems.register(journalWriterService, ZMQ.Poller.POLLIN);

        socketHandler = new Thread("ZeroMQJournal Socket Handler") {
            @Override
            public void run() {
                while (!isInterrupted()) {
                    try {
                        pollerItems.poll();
                        if (pollerItems.pollin(0)) {
                            handleJournalReaderService(journalReaderService.recv(0));
                        }
                        if (pollerItems.pollin(1)) {
                            handleJournalWriterService(journalWriterService.recv(0));
                        }
                    } catch (Throwable t) {
                        log.error(t.toString());
                    }
                }
            }
        };

        startBackgroundThreads();
    }

    @NotNull
    public static ZeroMQJournal newZeroMQJournal() throws IOException {
        final ZeroMQJournal zmqJournal = new ZeroMQJournal();
        return zmqJournal;
    }

    void handleJournalReaderService(byte[] msg) {
        while (true) {
            try {
                journalReaderService.send(head);
                break;
            } catch (Throwable t) {
                log.warn(t.toString());
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                }
            }
        }
    }

    void handleJournalWriterService(byte[] msg) {
        head = msg;
        final String sMsg = new String(msg);
        while (true) {
            try {
                journalWriterService.send(sMsg + " confirmed as new head.");
                break;
            } catch (Throwable t) {
                log.warn(t.toString());
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        stopBackgroundThreads();
        pollerItems.close();
        journalWriterService.close();
        journalReaderService.close();
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
