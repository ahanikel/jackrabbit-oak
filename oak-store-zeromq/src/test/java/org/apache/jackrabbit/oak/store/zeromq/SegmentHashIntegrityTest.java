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
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Verifies that the Merkle hash stored in every segment node record matches
 * the hash recomputed from the node's properties and children.
 */
public class SegmentHashIntegrityTest {

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
    public void tearDown() {
        threadPool.shutdownNow();
        store.close();
        context.close();
    }

    @Test
    public void hashIntegrityAfterCommit() throws CommitFailedException {
        NodeBuilder root = store.getRoot().builder();
        NodeBuilder content = root.child("content");
        content.setProperty("title", "Hello World");
        content.setProperty("count", 42L);
        NodeBuilder sub = content.child("sub");
        sub.setProperty("x", "nested");
        store.merge(root, new EmptyHook(), CommitInfo.EMPTY);

        NodeState savedRoot = store.getRoot();
        verifyHashIntegrity(savedRoot);
    }

    @Test
    public void hashIntegrityAfterMultipleCommits() throws CommitFailedException {
        NodeBuilder root = store.getRoot().builder();
        for (int i = 0; i < 5; i++) {
            root.child("node" + i).setProperty("val", (long) i);
        }
        store.merge(root, new EmptyHook(), CommitInfo.EMPTY);

        NodeState savedRoot = store.getRoot();
        verifyHashIntegrity(savedRoot);
    }

    /**
     * Recursively verify that every SegmentNodeState's stored Merkle hash
     * matches the recomputed hash from its properties and children.
     */
    private void verifyHashIntegrity(NodeState node) {
        if (!(node instanceof SegmentNodeState)) {
            return;
        }
        SegmentNodeState sns = (SegmentNodeState) node;
        String segmentId = sns.getSegmentId();
        if (segmentId == null || SegmentNodeState.NULL_HASH.equals(segmentId)) {
            return;
        }

        Segment seg = store.readSegment(segmentId);
        int recIdx = sns.getRecordIndex() == SegmentNodeState.ROOT_RECORD
                ? seg.getNodeCount() - 1
                : sns.getRecordIndex();
        Segment.NodeRecord rec = seg.getNodeRecord(recIdx);

        // Recompute Merkle hash
        List<PropertyState> propList = new ArrayList<>();
        for (PropertyState ps : node.getProperties()) {
            propList.add(ps);
        }

        List<MerkleHash.ChildEntry> childEntries = new ArrayList<>();
        for (ChildNodeEntry child : node.getChildNodeEntries()) {
            NodeState childState = child.getNodeState();
            if (childState instanceof SegmentNodeState) {
                SegmentNodeState childSns = (SegmentNodeState) childState;
                byte[] childHash = Util.hexToBytes(childSns.getRef());
                childEntries.add(new MerkleHash.ChildEntry(child.getName(), childHash));
            }
        }

        byte[] recomputed = MerkleHash.compute(propList, childEntries);
        assertArrayEquals(
                "Merkle hash mismatch for segment " + segmentId + " record " + recIdx,
                rec.merkleHash, recomputed);

        // Recurse into children
        for (ChildNodeEntry child : node.getChildNodeEntries()) {
            verifyHashIntegrity(child.getNodeState());
        }
    }
}
