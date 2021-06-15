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

import com.google.common.io.Files;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.store.zeromq.log.LogBackendStore;

import java.io.File;
import java.io.IOException;

public class ZeroMQFixture extends NodeStoreFixture {

    private ZeroMQNodeStore store;
    private ZeroMQBackendStore logBackendStore;
    private File logFile;
    private File cacheDir;

    public ZeroMQFixture() {
        try {
            logFile = File.createTempFile("ZeroMQFixture", ".log");
            cacheDir = Files.createTempDir();
            logBackendStore = LogBackendStore.builder()
                .withLogFile(logFile.getAbsolutePath())
                .withReaderUrl("tcp://*:8000")
                .withWriterUrl("tcp://*:8001")
                .withNumThreads(4)
                .build();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        store = ZeroMQNodeStore.builder()
            .setJournalId("test")
            .setBackendReaderURL("tcp://localhost:8000")
            .setBackendWriterURL("tcp://localhost:8001")
            .setBlobCacheDir(cacheDir.getAbsolutePath())
            .build();
    }

    @Override
    public NodeStore createNodeStore() {
        logFile.delete();
        cacheDir.delete();
        cacheDir.mkdir();
        store.reset();
        return store;
    }

    @Override
    public void dispose(NodeStore nodeStore) {
        store.close();
        logBackendStore.close();
        store = null;
        logBackendStore = null;
    }
}
