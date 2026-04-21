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

import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SegmentWriterTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private SimpleBlobStore blobStore;
    private SegmentWriter writer;

    @Before
    public void setup() throws IOException {
        File blobDir = tmp.newFolder();
        blobStore = new SimpleBlobStore(blobDir);
        writer = new SegmentWriter(blobStore);
    }

    private NodeState buildNode(Object... kvPairs) {
        MemoryNodeBuilder b = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        for (int i = 0; i < kvPairs.length; i += 2) {
            String name = (String) kvPairs[i];
            Object val = kvPairs[i + 1];
            if (val instanceof String) {
                b.setProperty(PropertyStates.createProperty(name, (String) val));
            } else if (val instanceof Long) {
                b.setProperty(PropertyStates.createProperty(name, (Long) val));
            } else if (val instanceof Boolean) {
                b.setProperty(PropertyStates.createProperty(name, (Boolean) val));
            }
        }
        return b.getNodeState();
    }

    @Test
    public void emptyNodeWriteProducesValidSegment() throws IOException {
        String segId = writer.write(EmptyNodeState.EMPTY_NODE);
        assertNotNull(segId);
        assertEquals(64, segId.length());
        assertTrue(segId.matches("[0-9a-f]{64}"));

        assertTrue(blobStore.hasBlob(segId));
        byte[] data = blobStore.getBytes(segId);
        Segment seg = Segment.parse(data);
        assertEquals(1, seg.getNodeCount());
        assertEquals(0, seg.getExtRefCount());
    }

    @Test
    public void segmentIdIsRootNodeMerkleHash() throws IOException {
        NodeState node = buildNode("x", "value");
        String segId = writer.write(node);
        byte[] data = blobStore.getBytes(segId);
        Segment seg = Segment.parse(data);

        // The root is always the last record (post-order DFS)
        int rootIdx = seg.getNodeCount() - 1;
        byte[] rootHash = seg.getNodeRecord(rootIdx).merkleHash;
        assertEquals(segId, Util.bytesToHex(rootHash));
    }

    @Test
    public void singleChildNode() throws IOException {
        MemoryNodeBuilder root = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        root.setChildNode("child", buildNode("p", "v"));
        String segId = writer.write(root.getNodeState());

        byte[] data = blobStore.getBytes(segId);
        Segment seg = Segment.parse(data);

        // Post-order: child at index 0, root at index 1
        assertEquals(2, seg.getNodeCount());
        Segment.NodeRecord rootRec = seg.getNodeRecord(seg.getNodeCount() - 1);
        assertEquals(1, rootRec.children.length);
        assertEquals("child", rootRec.children[0].name);
        assertFalse(rootRec.children[0].external);
    }

    @Test
    public void propertiesRoundTripViaSegment() throws IOException {
        NodeState node = buildNode("str", "hello", "num", 42L, "flag", true);
        String segId = writer.write(node);
        byte[] data = blobStore.getBytes(segId);
        Segment seg = Segment.parse(data);

        Segment.NodeRecord rec = seg.getNodeRecord(seg.getNodeCount() - 1);
        assertEquals(3, rec.properties.length);

        // Properties are stored in sorted order
        boolean foundStr = false, foundNum = false, foundFlag = false;
        for (Segment.ParsedProperty p : rec.properties) {
            switch (p.name) {
                case "str":
                    assertEquals("hello", new String(p.elements[0], java.nio.charset.StandardCharsets.UTF_8));
                    foundStr = true;
                    break;
                case "num":
                    assertEquals(42L, java.nio.ByteBuffer.wrap(p.elements[0]).getLong());
                    foundNum = true;
                    break;
                case "flag":
                    assertEquals((byte) 1, p.elements[0][0]);
                    foundFlag = true;
                    break;
            }
        }
        assertTrue(foundStr && foundNum && foundFlag);
    }

    @Test
    public void writeSameNodeTwiceIsDeterministic() throws IOException {
        NodeState node = buildNode("key", "same");
        String id1 = writer.write(node);
        String id2 = writer.write(node);
        assertEquals(id1, id2);
    }

    @Test
    public void deepTreeFitsInSingleSegmentBelowLimit() throws IOException {
        // Build a tree that fits comfortably within 256 KiB
        MemoryNodeBuilder root = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        for (int i = 0; i < 10; i++) {
            MemoryNodeBuilder child = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
            child.setProperty(PropertyStates.createProperty("idx", (long) i));
            root.setChildNode("child" + i, child.getNodeState());
        }
        String segId = writer.write(root.getNodeState());
        byte[] data = blobStore.getBytes(segId);
        Segment seg = Segment.parse(data);

        // 10 children + root = 11 nodes
        assertEquals(11, seg.getNodeCount());
        // All local refs — no external segments needed
        assertEquals(0, seg.getExtRefCount());
    }

    @Test
    public void oversizedChildSpillsToExternalSegment() throws IOException {
        // Create a writer with a tiny max segment size (500 bytes) to force overflow
        SegmentWriter smallWriter = new SegmentWriter(blobStore, 500);

        // Build a node with many children — each > 0 bytes, so we'll overflow quickly
        MemoryNodeBuilder root = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        for (int i = 0; i < 20; i++) {
            MemoryNodeBuilder child = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
            // Add enough data to each child to force overflow
            child.setProperty(PropertyStates.createProperty("data",
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345678901234567890123456789" + i));
            root.setChildNode("child" + String.format("%02d", i), child.getNodeState());
        }

        String rootSegId = smallWriter.write(root.getNodeState());
        byte[] data = blobStore.getBytes(rootSegId);
        Segment seg = Segment.parse(data);

        // The root segment should have at least one external ref
        assertTrue("Expected at least one external segment ref", seg.getExtRefCount() > 0);

        // Verify all external segments exist in the blob store
        for (int i = 0; i < seg.getExtRefCount(); i++) {
            String extId = seg.getExtRefHex(i);
            assertTrue("External segment " + extId + " should exist in blob store",
                    blobStore.hasBlob(extId));
        }
    }

    @Test
    public void emptyNodeSegmentIdMatchesEmptyHash() throws IOException {
        String segId = writer.write(EmptyNodeState.EMPTY_NODE);
        // Verify the segment ID equals SHA-256 of empty input
        assertEquals(Util.bytesToHex(MerkleHash.EMPTY_NODE_HASH), segId);
    }
}
