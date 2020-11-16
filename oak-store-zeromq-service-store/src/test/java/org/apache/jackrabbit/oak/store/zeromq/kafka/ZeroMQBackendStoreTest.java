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
package org.apache.jackrabbit.oak.store.zeromq.kafka;

import org.apache.jackrabbit.oak.store.zeromq.kafka.ZeroMQBackendStore;
import org.junit.Before;
import org.junit.Test;
import org.zeromq.ZMQ;

import static org.apache.jackrabbit.oak.store.zeromq.kafka.ZeroMQBackendStore.ZEROMQ_READER_PORT;
import static org.apache.jackrabbit.oak.store.zeromq.kafka.ZeroMQBackendStore.ZEROMQ_WRITER_PORT;
import static org.junit.Assert.assertEquals;

public class ZeroMQBackendStoreTest {
    private ZeroMQBackendStore store;
    private int readerPort;
    private int writerPort;
    private ZMQ.Context context;
    private ZMQ.Socket readerService;
    private ZMQ.Socket writerService;

    @Before
    public void testInit() {
        store = new ZeroMQBackendStore();
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
        context = ZMQ.context(2);
        readerService = context.socket(ZMQ.REQ);
        readerService.connect("tcp://localhost:" + readerPort);
        writerService = context.socket(ZMQ.REQ);
        writerService.connect("tcp://localhost:" + writerPort);
    }

    @Test
    public void testWriteRead() {
        writerService.send("hello\nworld");
        String response = writerService.recvStr();
        readerService.send("hello");
        assertEquals("world", readerService.recvStr());
    }
}
