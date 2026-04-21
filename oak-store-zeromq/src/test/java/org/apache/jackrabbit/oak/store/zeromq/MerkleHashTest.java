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

import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

public class MerkleHashTest {

    @Test
    public void emptyNodeHashIsKnownConstant() {
        byte[] actual = MerkleHash.compute(Collections.emptyList(), Collections.emptyList());
        assertArrayEquals(MerkleHash.EMPTY_NODE_HASH, actual);
        assertArrayEquals(Util.hexToBytes(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                actual);
    }

    @Test
    public void computeIsDeterministic() {
        List<MerkleHash.ChildEntry> children = Arrays.asList(
                new MerkleHash.ChildEntry("alpha", new byte[32]),
                new MerkleHash.ChildEntry("beta", new byte[32])
        );
        List<org.apache.jackrabbit.oak.api.PropertyState> props = Arrays.asList(
                PropertyStates.createProperty("x", "hello"),
                PropertyStates.createProperty("y", 42L)
        );

        byte[] h1 = MerkleHash.compute(props, children);
        byte[] h2 = MerkleHash.compute(props, children);
        assertArrayEquals(h1, h2);
    }

    @Test
    public void childrenAreSortedByName() {
        byte[] hashA = new byte[32];
        hashA[0] = 1;
        byte[] hashB = new byte[32];
        hashB[0] = 2;

        // Sorted order
        List<MerkleHash.ChildEntry> sorted = Arrays.asList(
                new MerkleHash.ChildEntry("aaa", hashA),
                new MerkleHash.ChildEntry("bbb", hashB)
        );
        // Reversed order
        List<MerkleHash.ChildEntry> reversed = Arrays.asList(
                new MerkleHash.ChildEntry("bbb", hashB),
                new MerkleHash.ChildEntry("aaa", hashA)
        );

        assertArrayEquals(
                MerkleHash.compute(Collections.emptyList(), sorted),
                MerkleHash.compute(Collections.emptyList(), reversed));
    }

    @Test
    public void propertiesAreSortedByName() {
        List<org.apache.jackrabbit.oak.api.PropertyState> ab = Arrays.asList(
                PropertyStates.createProperty("a", "v1"),
                PropertyStates.createProperty("b", "v2")
        );
        List<org.apache.jackrabbit.oak.api.PropertyState> ba = Arrays.asList(
                PropertyStates.createProperty("b", "v2"),
                PropertyStates.createProperty("a", "v1")
        );
        assertArrayEquals(
                MerkleHash.compute(ab, Collections.emptyList()),
                MerkleHash.compute(ba, Collections.emptyList()));
    }

    @Test
    public void differentPropertyValueProducesDifferentHash() {
        List<org.apache.jackrabbit.oak.api.PropertyState> propsA = Collections.singletonList(
                PropertyStates.createProperty("key", "value-a"));
        List<org.apache.jackrabbit.oak.api.PropertyState> propsB = Collections.singletonList(
                PropertyStates.createProperty("key", "value-b"));

        byte[] hA = MerkleHash.compute(propsA, Collections.emptyList());
        byte[] hB = MerkleHash.compute(propsB, Collections.emptyList());
        assertFalse(Arrays.equals(hA, hB));
    }

    @Test
    public void differentChildHashProducesDifferentHash() {
        byte[] childA = new byte[32];
        childA[0] = 0x01;
        byte[] childB = new byte[32];
        childB[0] = 0x02;

        byte[] hA = MerkleHash.compute(Collections.emptyList(),
                Collections.singletonList(new MerkleHash.ChildEntry("c", childA)));
        byte[] hB = MerkleHash.compute(Collections.emptyList(),
                Collections.singletonList(new MerkleHash.ChildEntry("c", childB)));
        assertFalse(Arrays.equals(hA, hB));
    }

    @Test
    public void addingChildChangesHash() {
        byte[] noChild = MerkleHash.compute(Collections.emptyList(), Collections.emptyList());
        byte[] withChild = MerkleHash.compute(Collections.emptyList(),
                Collections.singletonList(new MerkleHash.ChildEntry("c", new byte[32])));
        assertFalse(Arrays.equals(noChild, withChild));
    }
}
