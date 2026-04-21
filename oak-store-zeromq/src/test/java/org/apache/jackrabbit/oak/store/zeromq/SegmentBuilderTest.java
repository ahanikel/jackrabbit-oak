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

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SegmentBuilderTest {

    private static byte[] hash(int seed) {
        byte[] h = new byte[32];
        h[0] = (byte) seed;
        return h;
    }

    @Test
    public void emptySingleNodeRoundTrip() throws IOException {
        SegmentBuilder builder = new SegmentBuilder();
        byte[] merkle = hash(1);
        int idx = builder.addNodeRecord(merkle, Collections.emptyList(), Collections.emptyList());

        assertEquals(0, idx);
        byte[] bytes = builder.flush();

        Segment seg = Segment.parse(bytes);
        assertEquals(1, seg.getNodeCount());
        assertEquals(0, seg.getExtRefCount());

        Segment.NodeRecord rec = seg.getNodeRecord(0);
        assertArrayEquals(merkle, rec.merkleHash);
        assertEquals(0, rec.properties.length);
        assertEquals(0, rec.children.length);
    }

    @Test
    public void singlePropertyRoundTrip() throws IOException {
        SegmentBuilder builder = new SegmentBuilder();

        byte[] nameUtf8 = "myProp".getBytes(StandardCharsets.UTF_8);
        byte typeTag = 1; // STRING
        byte[] val = "hello".getBytes(StandardCharsets.UTF_8);
        SegmentBuilder.EncodedProperty prop =
                new SegmentBuilder.EncodedProperty(nameUtf8, typeTag, val.length, val);

        builder.addNodeRecord(hash(2), Collections.singletonList(prop), Collections.emptyList());
        byte[] bytes = builder.flush();
        Segment seg = Segment.parse(bytes);

        Segment.NodeRecord rec = seg.getNodeRecord(0);
        assertEquals(1, rec.properties.length);
        assertEquals("myProp", rec.properties[0].name);
        assertEquals(typeTag, rec.properties[0].typeTag);
        assertArrayEquals(val, rec.properties[0].elements[0]);
    }

    @Test
    public void localChildRefRoundTrip() throws IOException {
        SegmentBuilder builder = new SegmentBuilder();

        // child node at index 0
        builder.addNodeRecord(hash(10), Collections.emptyList(), Collections.emptyList());
        // parent at index 1 references child at index 0
        SegmentBuilder.PendingChild childRef = new SegmentBuilder.PendingChild("myChild", 0);
        builder.addNodeRecord(hash(11), Collections.emptyList(),
                Collections.singletonList(childRef));

        byte[] bytes = builder.flush();
        Segment seg = Segment.parse(bytes);

        assertEquals(2, seg.getNodeCount());
        Segment.NodeRecord parent = seg.getNodeRecord(1);
        assertEquals(1, parent.children.length);
        assertEquals("myChild", parent.children[0].name);
        assertFalse(parent.children[0].external);
        assertEquals(0, parent.children[0].recordIndex);
    }

    @Test
    public void externalChildRefRoundTrip() throws IOException {
        byte[] extHash = new byte[32];
        extHash[15] = (byte) 0xAB;
        String extHex = Util.bytesToHex(extHash);

        SegmentBuilder builder = new SegmentBuilder();
        SegmentBuilder.PendingChild extChild = new SegmentBuilder.PendingChild("extChild", extHash);
        builder.addNodeRecord(hash(20), Collections.emptyList(),
                Collections.singletonList(extChild));

        byte[] bytes = builder.flush();
        Segment seg = Segment.parse(bytes);

        assertEquals(1, seg.getNodeCount());
        assertEquals(1, seg.getExtRefCount());
        assertEquals(extHex, seg.getExtRefHex(0));

        Segment.NodeRecord rec = seg.getNodeRecord(0);
        assertEquals(1, rec.children.length);
        assertTrue(rec.children[0].external);
        assertEquals(extHex, rec.children[0].externalSegmentId);
    }

    @Test
    public void multipleNodesAndPropertiesRoundTrip() throws IOException {
        SegmentBuilder builder = new SegmentBuilder();

        // Node 0: leaf with two properties
        byte[] nameA = "alpha".getBytes(StandardCharsets.UTF_8);
        byte[] nameB = "beta".getBytes(StandardCharsets.UTF_8);
        byte[] valA = "aaa".getBytes(StandardCharsets.UTF_8);
        byte[] valB = new byte[]{0, 0, 0, 0, 0, 0, 0, 7}; // LONG 7
        SegmentBuilder.EncodedProperty propA = new SegmentBuilder.EncodedProperty(nameA, (byte)1, valA.length, valA);
        SegmentBuilder.EncodedProperty propB = new SegmentBuilder.EncodedProperty(nameB, (byte)5, valB.length, valB);
        int leafIdx = builder.addNodeRecord(hash(30), Arrays.asList(propA, propB),
                Collections.emptyList());
        assertEquals(0, leafIdx);

        // Node 1: parent with one child ref
        int parentIdx = builder.addNodeRecord(hash(31), Collections.emptyList(),
                Collections.singletonList(new SegmentBuilder.PendingChild("leaf", leafIdx)));
        assertEquals(1, parentIdx);

        byte[] bytes = builder.flush();
        Segment seg = Segment.parse(bytes);

        assertEquals(2, seg.getNodeCount());

        Segment.NodeRecord leaf = seg.getNodeRecord(0);
        assertEquals(2, leaf.properties.length);
        assertEquals("alpha", leaf.properties[0].name);
        assertArrayEquals(valA, leaf.properties[0].elements[0]);
        assertEquals("beta", leaf.properties[1].name);

        Segment.NodeRecord parent = seg.getNodeRecord(1);
        assertEquals(1, parent.children.length);
        assertEquals("leaf", parent.children[0].name);
        assertEquals(0, parent.children[0].recordIndex);
    }

    @Test
    public void currentTotalSizeGrowsMonotonically() throws IOException {
        SegmentBuilder builder = new SegmentBuilder();
        int prev = builder.currentTotalSize();
        for (int i = 0; i < 5; i++) {
            builder.addNodeRecord(hash(i), Collections.emptyList(), Collections.emptyList());
            int cur = builder.currentTotalSize();
            assertTrue(cur > prev);
            prev = cur;
        }
    }

    @Test
    public void arrayPropertyRoundTrip() throws IOException {
        SegmentBuilder builder = new SegmentBuilder();

        byte[] nameUtf8 = "tags".getBytes(StandardCharsets.UTF_8);
        // Array of 2 strings: "foo", "bar"
        byte[] foo = "foo".getBytes(StandardCharsets.UTF_8);
        byte[] bar = "bar".getBytes(StandardCharsets.UTF_8);
        // valueBytes for an array: [4-byte len | elem_bytes] * count
        java.io.ByteArrayOutputStream elemBuf = new java.io.ByteArrayOutputStream();
        java.io.DataOutputStream elemOut = new java.io.DataOutputStream(elemBuf);
        elemOut.writeInt(foo.length); elemOut.write(foo);
        elemOut.writeInt(bar.length); elemOut.write(bar);
        elemOut.flush();

        // typeTag = 0x81 (STRING | ARRAY bit)
        SegmentBuilder.EncodedProperty arrayProp =
                new SegmentBuilder.EncodedProperty(nameUtf8, (byte) (1 | 0x80), 2, elemBuf.toByteArray());

        builder.addNodeRecord(hash(40), Collections.singletonList(arrayProp),
                Collections.emptyList());
        byte[] bytes = builder.flush();
        Segment seg = Segment.parse(bytes);

        Segment.ParsedProperty p = seg.getNodeRecord(0).properties[0];
        assertEquals("tags", p.name);
        assertTrue(p.isArray);
        assertEquals(2, p.elements.length);
        assertArrayEquals(foo, p.elements[0]);
        assertArrayEquals(bar, p.elements[1]);
    }
}
