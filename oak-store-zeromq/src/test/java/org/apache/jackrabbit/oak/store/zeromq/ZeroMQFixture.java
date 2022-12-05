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

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ZeroMQFixture extends NodeStoreFixture {

    private static final Logger log = LoggerFactory.getLogger(ZeroMQFixture.class);
    private static final String publisherUrl = "ipc://comm-hub-pub";
    private static final String subscriberUrl = "ipc://comm-hub-sub";

    private File blobDir;
    private File blobCacheDir;
    private SimpleBlobReaderService reader;
    private SimpleBlobWriterService writer;
    private SimpleNodeStore store;
    private ZContext context;
    private ZMQ.Socket pubSocket;
    private ZMQ.Socket subSocket;
    private ExecutorService threadPool;

    public ZeroMQFixture() {
        try {
            context = new ZContext();
            pubSocket = context.createSocket(SocketType.PUB);
            pubSocket.bind(subscriberUrl);
            subSocket = context.createSocket(SocketType.SUB);
            subSocket.bind(publisherUrl);
            subSocket.subscribe("");
            blobDir = File.createTempFile("zeromqns", ".d");
            blobDir.delete();
            blobDir.mkdir();
            reader = new SimpleBlobReaderService(blobDir, publisherUrl, subscriberUrl);
            writer = new SimpleBlobWriterService(blobDir, publisherUrl, subscriberUrl);
            threadPool = Executors.newFixedThreadPool(3);
            threadPool.execute(() -> ZMQ.proxy(subSocket, pubSocket, null));
            threadPool.execute(reader);
            threadPool.execute(writer);
            blobCacheDir = File.createTempFile("zeromqns-cache", ".d");
            blobCacheDir.delete();
            blobCacheDir.mkdir();
            store = SimpleNodeStore.builder()
                    .setBackendReaderURL(subscriberUrl)
                    .setBackendWriterURL(publisherUrl)
                    .setJournalId("golden")
                    .setBlobCacheDir(blobCacheDir.getAbsolutePath())
                    .build();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /*
    @Override
    public void dispose(NodeStore ns) {
        threadPool.shutdownNow();
        store.close();
        store = null;
        reader = null;
        writer = null;
        context.close();
    }
    */

    @Override
    public NodeStore createNodeStore() {
        store.reset();
        return store;
    }
}
