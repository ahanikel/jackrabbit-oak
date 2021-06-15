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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;

public class ZeroMQFixture extends NodeStoreFixture {

    private static class Store {
        private ZeroMQNodeStore store;
        private ZeroMQBackendStore logBackendStore;
        private File logFile;
        private File cacheDir;
        private int readerPort;
        private int writerPort;
        private boolean borrowedBackend = false;

        public ZeroMQNodeStore getStore() {
            return store;
        }

        public Store withStore(ZeroMQNodeStore store) {
            this.store = store;
            return this;
        }

        public ZeroMQBackendStore getLogBackendStore() {
            return logBackendStore;
        }

        public Store withLogBackendStore(ZeroMQBackendStore logBackendStore) {
            this.logBackendStore = logBackendStore;
            return this;
        }

        public File getLogFile() {
            return logFile;
        }

        public Store withLogFile(File logFile) {
            this.logFile = logFile;
            return this;
        }

        public File getCacheDir() {
            return cacheDir;
        }

        public Store withCacheDir(File cacheDir) {
            this.cacheDir = cacheDir;
            return this;
        }

        public int getReaderPort() {
            return readerPort;
        }

        public Store withReaderPort(int readerPort) {
            this.readerPort = readerPort;
            return this;
        }

        public int getWriterPort() {
            return writerPort;
        }

        public Store withWriterPort(int writerPort) {
            this.writerPort = writerPort;
            return this;
        }

        public boolean hasBorrowedBackend() {
            return borrowedBackend;
        }

        public Store withBorrowedBackend(boolean borrowedBackend) {
            this.borrowedBackend = borrowedBackend;
            return this;
        }

        private Store() {
        }

        public Store build() throws FileNotFoundException {
            if (!borrowedBackend) {
                withLogBackendStore(
                    LogBackendStore.builder()
                        .withLogFile(logFile.getAbsolutePath())
                        .withReaderUrl("tcp://*:" + getReaderPort())
                        .withWriterUrl("tcp://*:" + getWriterPort())
                        .withNumThreads(4)
                        .build());
            }
            withStore(
                ZeroMQNodeStore.builder()
                    .setJournalId("test")
                    .setBackendReaderURL("tcp://localhost:" + getReaderPort())
                    .setBackendWriterURL("tcp://localhost:" + getWriterPort())
                    .setBlobCacheDir(cacheDir.getAbsolutePath())
                    .setRemoteReads(true)
                    .setWriteBackJournal(true)
                    .setWriteBackNodes(true)
                    .build());
            return this;
        }

        public void reset() {
            logFile.delete();
            cacheDir.delete();
            cacheDir.mkdir();
            store.reset();
        }

        public void close() {
            store.close();
            if (!hasBorrowedBackend()) {
                logBackendStore.close();
                logBackendStore = null;
            }
            store = null;
        }
    }

    private Map<ZeroMQNodeStore, Store> stores = new HashMap<>();

    public ZeroMQFixture() {}

    @Override
    public NodeStore createNodeStore() {
        try {
            Store newStore = new Store()
                .withReaderPort(unusedRandomPort())
                .withWriterPort(unusedRandomPort())
                .withLogFile(File.createTempFile("ZeroMQFixture", ".log"))
                .withCacheDir(Files.createTempDir())
                .build();
            stores.put(newStore.getStore(), newStore);
            return newStore.getStore();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void dispose(NodeStore nodeStore) {
        Store s = stores.remove(nodeStore);
        if (s != null) {
            s.close();
        }
    }

    public ZeroMQNodeStore createNodeStoreWithSameBackendAs(NodeStore otherStore) {
        Store other = stores.get(otherStore);
        for (String otherRoot = ((ZeroMQNodeStore) otherStore).getSuperRoot().getUuid();
             otherRoot.equals("undefined") || otherRoot.equals(other.getStore().emptyNode.getUuid());
             otherRoot = ((ZeroMQNodeStore) otherStore).getSuperRoot().getUuid()) {
            try {
                Thread.sleep(100); // wait until otherStore has finished initialising
            } catch (InterruptedException e) {
            }
        }
        Store newStore;
        try {
            newStore = new Store()
                .withBorrowedBackend(true)
                .withReaderPort(other.getReaderPort())
                .withWriterPort(other.getWriterPort())
                .withCacheDir(Files.createTempDir())
                .build();
            stores.put(newStore.getStore(), newStore);
            return newStore.getStore();
        } catch (FileNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    public static int unusedRandomPort() {
        ServerSocket s = null;
        try {
            s = new ServerSocket(0);
            return s.getLocalPort();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (s != null) {
                    s.close();
                }
            } catch (IOException e) {
            }
        }
    }
}
