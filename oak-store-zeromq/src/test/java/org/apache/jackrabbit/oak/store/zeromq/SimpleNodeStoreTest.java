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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Ignore("unfinished")
public class SimpleNodeStoreTest {

    private static final String publisherUrl = "ipc://comm-hub-pub";
    private static final String subscriberUrl = "ipc://comm-hub-sub";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private File blobDir;
    private SimpleBlobReaderService reader;
    private SimpleNodeStateWriterService writer;
    private SimpleNodeStore store;
    private ZContext context;
    private ZMQ.Socket pubSocket;
    private ZMQ.Socket subSocket;
    private ExecutorService threadPool;

    @Before
    public void setup() throws IOException {
        context = new ZContext();
        pubSocket = context.createSocket(SocketType.PUB);
        pubSocket.bind(subscriberUrl);
        subSocket = context.createSocket(SocketType.SUB);
        subSocket.bind(publisherUrl);
        subSocket.subscribe("");
        blobDir = temporaryFolder.newFolder();
        reader = new SimpleBlobReaderService(blobDir, publisherUrl, subscriberUrl);
        writer = new SimpleNodeStateWriterService(blobDir, publisherUrl, subscriberUrl);
        threadPool = Executors.newFixedThreadPool(3);
        threadPool.execute(() -> ZMQ.proxy(subSocket, pubSocket, null));
        threadPool.execute(reader);
        threadPool.execute(writer);
        store = SimpleNodeStore.builder()
            .setBackendReaderURL(subscriberUrl)
            .setBackendWriterURL(publisherUrl)
            .setJournalId("golden")
            .setBlobCacheDir(temporaryFolder.newFolder().getAbsolutePath())
            .build();
    }

    @After
    public void tearDown() throws InterruptedException {
        threadPool.shutdownNow();
        store.close();
        store = null;
        reader = null;
        writer = null;
        context.close();
    }

    @Test
    public void testInit() throws CommitFailedException {
        NodeBuilder root = store.getRoot().builder();
        NodeBuilder child = root.child("content");
        child.setProperty("a-string", "the-value");
        child.setProperty("a-long", 99);
        child.setProperty("a-binary", "the-value".getBytes(StandardCharsets.UTF_8));
        store.merge(root, new EmptyHook(), CommitInfo.EMPTY);
    }

    @Test
    public void testCheckpoints() {
        final String cpRef = store.checkpoint(10000);
        System.out.println("cpRef is " + cpRef);
        for (String cp : store.checkpoints()) {
            System.out.println(cp);
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        System.out.println("After 1 second: ");
        for (String cp : store.checkpoints()) {
            System.out.println(cp);
        }
    }
}
