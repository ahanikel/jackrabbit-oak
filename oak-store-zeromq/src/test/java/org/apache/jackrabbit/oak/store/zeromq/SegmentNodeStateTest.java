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
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SegmentNodeStateTest {

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

    @Before
    public void setup() throws IOException {
        ServerSocket pubSock = new ServerSocket(0);
        publisherUrl = "tcp://localhost:" + pubSock.getLocalPort();
        ServerSocket subSock = new ServerSocket(0);
        subscriberUrl = "tcp://localhost:" + subSock.getLocalPort();
        pubSock.close();
        subSock.close();

        context = new ZContext();
        pubSocket = context.createSocket(SocketType.PUB);
        pubSocket.bind(subscriberUrl);
        subSocket = context.createSocket(SocketType.SUB);
        subSocket.bind(publisherUrl);
        subSocket.subscribe("");

        blobDir = temporaryFolder.newFolder();
        reader = new SimpleBlobReaderService(new SimpleBlobStore(blobDir), publisherUrl, subscriberUrl);
        writer = new SimpleBlobWriterService(new SimpleBlobStore(blobDir), publisherUrl, subscriberUrl);

        threadPool = Executors.newFixedThreadPool(3);
        threadPool.execute(() -> ZMQ.proxy(subSocket, pubSocket, null));
        threadPool.execute(reader);
        threadPool.execute(writer);

        store = SimpleNodeStore.builder()
                .setBackendReaderURL(subscriberUrl)
                .setBackendWriterURL(publisherUrl)
                .setJournalId(Constants.DEFAULT_JOURNAL_ID)
                .setBlobCacheDir(temporaryFolder.newFolder().getAbsolutePath())
                .build();
    }

    @After
    public void tearDown() throws InterruptedException {
        threadPool.shutdownNow();
        store.close();
        context.close();
    }

    @Test
    public void emptyNodeStateExists() {
        SegmentNodeState empty = store.EMPTY;
        assertTrue(empty.exists());
        assertEquals(0, empty.getPropertyCount());
        assertEquals(0, empty.getChildNodeCount(Integer.MAX_VALUE));
    }

    @Test
    public void missingNodeStateDoesNotExist() {
        SegmentNodeState missing = store.MISSING;
        assertFalse(missing.exists());
    }

    @Test
    public void nullHashIsZeros() {
        assertEquals(64, SegmentNodeState.NULL_HASH.length());
        assertTrue(SegmentNodeState.NULL_HASH.matches("0{64}"));
    }

    @Test
    public void stringPropertyRoundTrip() throws CommitFailedException {
        NodeBuilder root = store.getRoot().builder();
        root.setProperty("greeting", "hello");
        store.merge(root, new EmptyHook(), CommitInfo.EMPTY);

        NodeState saved = store.getRoot();
        assertEquals("hello", saved.getString("greeting"));
    }

    @Test
    public void longPropertyRoundTrip() throws CommitFailedException {
        NodeBuilder root = store.getRoot().builder();
        root.setProperty("count", 12345L);
        store.merge(root, new EmptyHook(), CommitInfo.EMPTY);

        NodeState saved = store.getRoot();
        assertEquals(12345L, saved.getLong("count"));
    }

    @Test
    public void booleanPropertyRoundTrip() throws CommitFailedException {
        NodeBuilder root = store.getRoot().builder();
        root.setProperty("flag", true);
        store.merge(root, new EmptyHook(), CommitInfo.EMPTY);

        NodeState saved = store.getRoot();
        assertTrue(saved.getBoolean("flag"));
    }

    @Test
    public void binaryPropertyRoundTrip() throws CommitFailedException, IOException {
        byte[] data = "binary-content".getBytes(StandardCharsets.UTF_8);
        NodeBuilder root = store.getRoot().builder();
        root.setProperty("blob", data);
        store.merge(root, new EmptyHook(), CommitInfo.EMPTY);

        NodeState saved = store.getRoot();
        assertNotNull(saved.getProperty("blob"));
        byte[] readBack = saved.getProperty("blob").getValue(Type.BINARY)
                .getNewStream().readAllBytes();
        assertArrayEquals(data, readBack);
    }

    private void assertArrayEquals(byte[] expected, byte[] actual) {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals("Byte at index " + i, expected[i], actual[i]);
        }
    }

    @Test
    public void childNodeRoundTrip() throws CommitFailedException {
        NodeBuilder root = store.getRoot().builder();
        NodeBuilder child = root.child("myChild");
        child.setProperty("x", "value");
        store.merge(root, new EmptyHook(), CommitInfo.EMPTY);

        NodeState saved = store.getRoot();
        assertTrue(saved.hasChildNode("myChild"));
        NodeState childState = saved.getChildNode("myChild");
        assertTrue(childState.exists());
        assertEquals("value", childState.getString("x"));
    }

    @Test
    public void missingChildNodeDoesNotExist() throws CommitFailedException {
        NodeState root = store.getRoot();
        NodeState missing = root.getChildNode("nonexistent");
        assertFalse(missing.exists());
    }

    @Test
    public void getPropertyReturnsNullForMissingProperty() throws CommitFailedException {
        NodeState root = store.getRoot();
        assertNull(root.getProperty("no-such-property"));
    }

    @Test
    public void equalNodesHaveSameRef() throws CommitFailedException {
        NodeBuilder root = store.getRoot().builder();
        root.setProperty("x", "same");
        store.merge(root, new EmptyHook(), CommitInfo.EMPTY);

        NodeState r1 = store.getRoot();
        NodeState r2 = store.getRoot();
        assertEquals(r1, r2);
        assertEquals(((SegmentNodeState) r1).getRef(),
                ((SegmentNodeState) r2).getRef());
    }

    @Test
    public void compareAgainstSameRefIsNoop() throws CommitFailedException {
        NodeBuilder root = store.getRoot().builder();
        root.setProperty("p", "val");
        store.merge(root, new EmptyHook(), CommitInfo.EMPTY);

        NodeState r = store.getRoot();
        // Comparing a SegmentNodeState against itself should return true immediately
        boolean result = r.compareAgainstBaseState(r, new org.apache.jackrabbit.oak.spi.state.NodeStateDiff() {
            @Override public boolean propertyAdded(org.apache.jackrabbit.oak.api.PropertyState after) { return false; }
            @Override public boolean propertyChanged(org.apache.jackrabbit.oak.api.PropertyState before, org.apache.jackrabbit.oak.api.PropertyState after) { return false; }
            @Override public boolean propertyDeleted(org.apache.jackrabbit.oak.api.PropertyState before) { return false; }
            @Override public boolean childNodeAdded(String name, NodeState after) { return false; }
            @Override public boolean childNodeChanged(String name, NodeState before, NodeState after) { return false; }
            @Override public boolean childNodeDeleted(String name, NodeState before) { return false; }
        });
        // Should return true (no changes) without calling any diff methods
        assertTrue(result);
    }
}
