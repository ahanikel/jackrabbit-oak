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

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class SimpleNodeStoreTest {

    private static final String publisherUrl = "ipc://comm-hub-pub";
    private static final String subscriberUrl = "ipc://comm-hub-sub";
    private static final Logger log = LoggerFactory.getLogger(SimpleNodeStoreTest.class);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private File blobDir;
    private SimpleBlobReaderService reader;
    private SimpleBlobWriterService writer;
    private SimpleNodeStore store;
    private ZContext context;
    private ZMQ.Socket pubSocket;
    private ZMQ.Socket subSocket;
    private ExecutorService threadPool;

    private SimpleNodeStore newSimpleNodeStore(String journalId) throws IOException {
        return SimpleNodeStore.builder()
                .setBackendReaderURL(subscriberUrl)
                .setBackendWriterURL(publisherUrl)
                .setJournalId(journalId)
                .setBlobCacheDir(temporaryFolder.newFolder().getAbsolutePath())
                .build();
    }

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
        writer = new SimpleBlobWriterService(blobDir, publisherUrl, subscriberUrl);
        threadPool = Executors.newFixedThreadPool(3);
        threadPool.execute(() -> ZMQ.proxy(subSocket, pubSocket, null));
        threadPool.execute(reader);
        threadPool.execute(writer);
        store = newSimpleNodeStore("golden");
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
    public void testConcurrentReaders() {
        SimpleRequestResponse reader1 = new SimpleRequestResponse(SimpleRequestResponse.Topic.READ, publisherUrl, subscriberUrl);
        SimpleRequestResponse reader2 = new SimpleRequestResponse(SimpleRequestResponse.Topic.READ, publisherUrl, subscriberUrl);
        reader1.requestString("journal", "golden");
        reader1.receiveMore();
        String ret1 = reader1.requestString("journal", "golden");
        String ret2 = reader2.requestString("journal", "golden");
        String ret4 = reader2.receiveMore();
        String ret3 = reader1.receiveMore();
        Assert.assertEquals("E", ret1);
        Assert.assertEquals("E", ret2);
        Assert.assertEquals("953A5B0E88B832A0122DB4EA380D6E12", ret3);
        Assert.assertEquals("953A5B0E88B832A0122DB4EA380D6E12", ret4);

        String rethas1 = reader1.requestString("hasblob", "953A5B0E88B832A0122DB4EA380D6E12");
        Assert.assertEquals("E", rethas1);
        String rethas2 = reader1.receiveMore();
        Assert.assertTrue(Boolean.valueOf(rethas2));

        String retblob1 = reader1.requestString("blob", "953A5B0E88B832A0122DB4EA380D6E12");
        Assert.assertEquals("E", retblob1);
        String retblob2 = reader1.receiveMore();
        Assert.assertEquals("n:\n" +
                "n+ checkpoints 00000000-0000-0000-0000-000000000000\n" +
                "n+ root 00000000-0000-0000-0000-000000000000\n" +
                "n!\n", retblob2);
    }

    @Test
    public void testConcurrentWriters() throws IOException, InterruptedException {
        SimpleNodeStore store2 = newSimpleNodeStore("golden");
        Random random = new Random();
        byte[] randomBlob = new byte[1024576];
        for (int i = 0; i < randomBlob.length; ++i) {
            randomBlob[i] = (byte) random.nextInt(256);
        }
        AtomicReference<Blob> blob1 = new AtomicReference<>();
        AtomicReference<Blob> blob2 = new AtomicReference<>();
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute( () -> {
            try {
                blob1.set(store.createBlob(new ByteArrayInputStream(randomBlob)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.execute( () -> {
            try {
                blob2.set(store.createBlob(new ByteArrayInputStream(randomBlob)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        log.info("blobs written");
        Assert.assertEquals(blob1.get().getReference(), blob2.get().getReference());
        Assert.assertTrue(store.getBlob(blob1.get().getReference()) != null);
        Assert.assertTrue(store2.getBlob(blob2.get().getReference()) != null);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(1024576);
        IOUtils.copy(store.getBlob(blob1.get().getReference()).getNewStream(), bos);
        Assert.assertArrayEquals(randomBlob, bos.toByteArray());
    }

    /*
    This sometimes fails with 23:06:55.372 [main] ERROR org.apache.jackrabbit.oak.store.zeromq.SimpleRequestResponse - Request id does not match, actual: [0, 0, 0, 0, 0, 0, 0, 0], expected: [0, 0, 0, 0, 0, 0, 0, 2]
    java.lang.IllegalStateException: java.io.FileNotFoundException: /var/folders/4k/0rck0w3s49vbwm062m2yx39c0000gn/T/junit14112334235295906039/junit15436517003978018593/8E/29/9F/8E299FCB7AF0105D714D1FF1637E3D5B (No such file or directory)
    */
    @Test
    public void testConcurrent() throws IOException, CommitFailedException, InterruptedException {
        SimpleNodeStore store2 = newSimpleNodeStore("golden");

        NodeBuilder root = store.getRoot().builder();
        NodeBuilder root2 = store2.getRoot().builder();

        NodeBuilder child = root.child("content");
        child.setProperty("a-string", "the-value");
        child.setProperty("a-long", 99);
        child.setProperty("a-binary", "the-value".getBytes(StandardCharsets.UTF_8));

        NodeBuilder child2 = root.child("content");
        child2.setProperty("a-string", "the-value");
        child2.setProperty("a-long", 97);
        child2.setProperty("a-binary", "the-value".getBytes(StandardCharsets.UTF_8));

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(() -> {
            try {
                store.merge(root, new EmptyHook(), CommitInfo.EMPTY);
            } catch (CommitFailedException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.execute(() -> {
            try {
                store2.merge(root, new EmptyHook(), CommitInfo.EMPTY);
            } catch (CommitFailedException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        store2.close();
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
