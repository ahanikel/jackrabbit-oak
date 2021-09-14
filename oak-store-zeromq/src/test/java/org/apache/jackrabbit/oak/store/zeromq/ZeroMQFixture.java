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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;

public class ZeroMQFixture extends NodeStoreFixture {

    private static Logger log = LoggerFactory.getLogger(ZeroMQFixture.class);
    private ZeroMQNodeStore store;
    private ZeroMQBackendStore logBackendStore;
    private File logFile;
    private File cacheDir;

    public ZeroMQFixture() {
    }

    @Override
    public NodeStore createNodeStore() {
        try {
            logFile = File.createTempFile("ZeroMQFixture", ".log");
            cacheDir = Files.createTempDir();
            logBackendStore = LogBackendStore.builder()
                .withLogFile(logFile.getAbsolutePath())
                .withReaderUrl("ipc:///tmp/fixtureBackendReader")
                .withWriterUrl("ipc:///tmp/fixtureBackendWriter")
                .withNumThreads(4)
                .build();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        store = ZeroMQNodeStore.builder()
            .setJournalId("test")
            .setBackendReaderURL("ipc:///tmp/fixtureBackendReader")
            .setBackendWriterURL("ipc:///tmp/fixtureBackendWriter")
            .setBlobCacheDir(cacheDir.getAbsolutePath())
            .setWriteBackNodes(true)
            .setWriteBackJournal(true)
            .build();

        store.reset();

        log.info("LogFile: {}", logFile.getAbsolutePath());
        log.info("CacheDir: {}", cacheDir.getAbsolutePath());
        return store;
    }

    @Override
    public void dispose(NodeStore nodeStore) {
        if (store != null) {
            store.close();
            store = null;
        }
        if (logBackendStore != null) {
            logBackendStore.close();
            logBackendStore = null;
        }
    }

    public File getLogFile() {
        return logFile;
    }

    public File getCacheDir() {
        return cacheDir;
    }
}
