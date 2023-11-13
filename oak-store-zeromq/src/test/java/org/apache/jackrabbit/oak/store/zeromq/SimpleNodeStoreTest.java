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
import org.apache.jackrabbit.oak.jcr.Jcr;
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

import javax.jcr.*;
import javax.jcr.lock.Lock;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SimpleNodeStoreTest {

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
    private String publisherUrl;
    private String subscriberUrl;

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
        // I used ipc://comm-hub-pub and ipc://comm-hub-sub but that doesn't
        // seem to work on all machines
        ServerSocket sock = new ServerSocket(0);
        publisherUrl = "tcp://localhost:" + sock.getLocalPort();
        sock.close();
        sock = new ServerSocket(0);
        subscriberUrl = "tcp://localhost:" + sock.getLocalPort();
        sock.close();
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
        assertTrue(Boolean.valueOf(rethas2));

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
        assertTrue(store.getBlob(blob1.get().getReference()) != null);
        assertTrue(store2.getBlob(blob2.get().getReference()) != null);
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

        NodeBuilder child2 = root2.child("content");
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
                store2.merge(root2, new EmptyHook(), CommitInfo.EMPTY);
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

    @Test
    public void testJcr() throws RepositoryException, IOException {
        final Jcr jcr1 = new Jcr(store);
        final Repository repo1 = jcr1.createRepository();
        final Session session1 = repo1.login(new SimpleCredentials("admin", "admin".toCharArray()));
        final Node test1 = session1.getRootNode().addNode("test");
        test1.addMixin("mix:lockable");
        session1.save();
        assertFalse(test1.isLocked());

        final SimpleNodeStore store2 = newSimpleNodeStore("golden");
        final Jcr jcr2 = new Jcr(store2);
        final Repository repo2 = jcr2.createRepository();
        final Session session2 = repo2.login(new SimpleCredentials("admin", "admin".toCharArray()));
        final Node test2 = session2.getRootNode().getNode("test");
        assertFalse(test2.isLocked());

        Lock lock = test1.lock(false, false);
        assertTrue(test1.isLocked());
        assertFalse(test2.isLocked());
        session2.refresh(true);
        final Node test3 = session2.getRootNode().getNode("test");
        assertTrue(test3.isLocked());
        test1.unlock();
        assertFalse(test1.isLocked());
        session2.logout();
        store2.close();
    }
}
