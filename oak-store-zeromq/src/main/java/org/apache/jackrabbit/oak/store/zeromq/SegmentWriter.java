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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static javax.jcr.PropertyType.*;

/**
 * Recursive DFS segment writer.
 * Walks a node tree post-order, packs nodes into binary segments, and writes
 * each segment to the blob store by its SHA-256 segment ID.
 */
public class SegmentWriter {

    public static final int DEFAULT_MAX_SEGMENT_SIZE = 2 * 1024 * 1024; // 2 MiB

    private final BlobStore blobStore;
    private final int maxSegmentSize;

    public SegmentWriter(BlobStore blobStore) {
        this(blobStore, DEFAULT_MAX_SEGMENT_SIZE);
    }

    public SegmentWriter(BlobStore blobStore, int maxSegmentSize) {
        this.blobStore = blobStore;
        this.maxSegmentSize = maxSegmentSize;
    }

    /**
     * Result returned by {@link #writeSubtree}.
     */
    static class WriteResult {
        final byte[] merkleHash;   // 32 bytes
        final boolean local;
        final int recordIndex;     // valid if local == true
        final String segmentId;    // 64-char hex; valid if local == false
        final List<SegmentBuilder.EncodedProperty> encodedProps; // cached binary props

        WriteResult(byte[] merkleHash, int recordIndex,
                    List<SegmentBuilder.EncodedProperty> encodedProps) {
            this.merkleHash = merkleHash;
            this.local = true;
            this.recordIndex = recordIndex;
            this.segmentId = null;
            this.encodedProps = encodedProps;
        }

        WriteResult(byte[] merkleHash, String segmentId,
                    List<SegmentBuilder.EncodedProperty> encodedProps) {
            this.merkleHash = merkleHash;
            this.local = false;
            this.recordIndex = -1;
            this.segmentId = segmentId;
            this.encodedProps = encodedProps;
        }
    }

    /** Output of {@link #writeFull}. */
    public static class WriteOutput {
        public final String segId;
        public final byte[] segBytes;
        WriteOutput(String segId, byte[] segBytes) {
            this.segId = segId;
            this.segBytes = segBytes;
        }
    }

    /**
     * Write the node tree rooted at {@code root} to the blob store and return
     * the raw segment bytes alongside the segment ID.  The caller can parse and
     * cache the segment without a second round-trip to the blob store.
     */
    public WriteOutput writeFull(NodeState root) throws IOException {
        SegmentBuilder builder = new SegmentBuilder();
        WriteResult result = writeSubtree(root, builder);
        byte[] segBytes = builder.flush();
        String segId = Util.bytesToHex(result.merkleHash);
        storeSegment(segId, segBytes);
        return new WriteOutput(segId, segBytes);
    }

    /**
     * Write the node tree rooted at {@code root} to the blob store.
     *
     * @return 64-char hex segment ID of the root segment
     */
    public String write(NodeState root) throws IOException {
        return writeFull(root).segId;
    }

    /**
     * Recursive post-order DFS.
     */
    WriteResult writeSubtree(NodeState node, SegmentBuilder builder) throws IOException {
        // Step 1: encode properties (cached for size estimation + hash + writing)
        List<PropertyState> propList = StreamSupport
                .stream(node.getProperties().spliterator(), false)
                .sorted((a, b) -> a.getName().compareTo(b.getName()))
                .collect(Collectors.toList());

        List<SegmentBuilder.EncodedProperty> encodedProps = new ArrayList<>();
        for (PropertyState ps : propList) {
            encodedProps.add(encodeProperty(ps));
        }

        // Step 2: recursively process children
        List<SegmentBuilder.PendingChild> pendingChildren = new ArrayList<>();
        List<MerkleHash.ChildEntry> merkleChildren = new ArrayList<>();

        List<String> childNames = StreamSupport
                .stream(node.getChildNodeNames().spliterator(), false)
                .sorted()
                .collect(Collectors.toList());

        for (String childName : childNames) {
            NodeState child = node.getChildNode(childName);

            SegmentBuilder.PendingChild pendingChild;
            byte[] childHash;

            if (child instanceof SegmentNodeState
                    && ((SegmentNodeState) child).getRecordIndex() == SegmentNodeState.ROOT_RECORD) {
                // Fast path: this child is already the root of a persisted external segment.
                // The segment ID is hex(merkleHash) by construction, so we can derive the hash
                // without loading the segment and add it as an external reference directly.
                childHash = Util.hexToBytes(((SegmentNodeState) child).getSegmentId());
                pendingChild = new SegmentBuilder.PendingChild(childName, childHash);
            } else {
                int estimatedChildSize = estimateNodeSize(child);
                if (builder.currentTotalSize() + estimatedChildSize > maxSegmentSize) {
                    // Child gets its own segment
                    SegmentBuilder childBuilder = new SegmentBuilder();
                    WriteResult childResult = writeSubtree(child, childBuilder);
                    byte[] segBytes = childBuilder.flush();
                    String childSegId = Util.bytesToHex(childResult.merkleHash);
                    storeSegment(childSegId, segBytes);
                    childHash = childResult.merkleHash;
                    pendingChild = new SegmentBuilder.PendingChild(childName, childHash);
                } else {
                    WriteResult childResult = writeSubtree(child, builder);
                    childHash = childResult.merkleHash;
                    pendingChild = new SegmentBuilder.PendingChild(childName, childResult.recordIndex);
                }
            }

            pendingChildren.add(pendingChild);
            merkleChildren.add(new MerkleHash.ChildEntry(childName, childHash));
        }

        // Step 3: compute Merkle hash
        byte[] merkleHash = MerkleHash.compute(propList, merkleChildren);

        // Step 4: add record to builder
        int recordIndex = builder.addNodeRecord(merkleHash, encodedProps, pendingChildren);

        return new WriteResult(merkleHash, recordIndex, encodedProps);
    }

    private void storeSegment(String segmentId, byte[] segBytes) throws IOException {
        if (!blobStore.hasBlob(segmentId)) {
            blobStore.putInputStreamAs(segmentId, new ByteArrayInputStream(segBytes));
        }
    }

    /**
     * Estimate the in-memory footprint of {@code node} and its descendants.
     * {@link SegmentNodeState} subtrees are already persisted on disk and
     * contribute 0 to the estimate — only genuine in-memory nodes are counted.
     * This lets callers decide whether a builder needs to be flushed to a
     * segment to reclaim memory.
     */
    public static int estimateInMemorySize(NodeState node) {
        if (node instanceof SegmentNodeState) {
            return 0; // already on disk
        }
        // Fixed per-record cost in the segment binary format
        int size = Segment.HEADER_SIZE + 8 + 32 + 2 + 2;
        for (PropertyState ps : node.getProperties()) {
            int nameLen = ps.getName().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            size += 2 + nameLen + 1 + 4 + estimateValueSizeStatic(ps);
        }
        for (String childName : node.getChildNodeNames()) {
            int nameLen = childName.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            size += 2 + nameLen + 1 + 4; // child ref overhead
            size += estimateInMemorySize(node.getChildNode(childName));
        }
        return size;
    }

    private static int estimateValueSizeStatic(PropertyState ps) {
        if (ps.isArray()) {
            int total = 0;
            for (int i = 0; i < ps.count(); i++) {
                total += 4 + estimateScalarSizeStatic(ps, i);
            }
            return total;
        }
        return estimateScalarSizeStatic(ps, 0);
    }

    private static int estimateScalarSizeStatic(PropertyState ps, int index) {
        int tag = ps.getType().isArray() ? ps.getType().getBaseType().tag() : ps.getType().tag();
        switch (tag) {
            case LONG:
            case DOUBLE: return 8;
            case BOOLEAN: return 1;
            case BINARY: return 64;
            default:
                String s = ps.getValue(org.apache.jackrabbit.oak.api.Type.STRING, index);
                return s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }
    }

    /**
     * Rough size estimate for a subtree (just this node's properties + child ref overheads).
     * Used to decide whether to spill into a new segment.
     */
    private int estimateNodeSize(NodeState node) {
        int size = 32 + 2 + 2; // merkle_hash + counts
        for (PropertyState ps : node.getProperties()) {
            int nameLen = ps.getName().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            size += 2 + nameLen + 1 + 4 + estimateValueSize(ps);
        }
        for (String childName : node.getChildNodeNames()) {
            int nameLen = childName.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            size += 2 + nameLen + 1 + 4; // treat children as external refs for estimation
        }
        return size;
    }

    private int estimateValueSize(PropertyState ps) {
        if (ps.isArray()) {
            int total = 0;
            for (int i = 0; i < ps.count(); i++) {
                total += 4 + estimateScalarSize(ps, i);
            }
            return total;
        }
        return estimateScalarSize(ps, 0);
    }

    private int estimateScalarSize(PropertyState ps, int index) {
        int tag = ps.getType().isArray() ? ps.getType().getBaseType().tag() : ps.getType().tag();
        switch (tag) {
            case LONG:
            case DOUBLE: return 8;
            case BOOLEAN: return 1;
            case BINARY: return 64;
            default:
                String s = ps.getValue(Type.STRING, index);
                return s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }
    }

    /**
     * Binary-encode a property for storage in a segment.
     * BINARY properties with inline blobs (no reference) are uploaded to the
     * blob store so the segment always stores a stable reference string.
     */
    SegmentBuilder.EncodedProperty encodeProperty(PropertyState ps) throws IOException {
        byte[] nameUtf8 = ps.getName().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        boolean array = ps.isArray();
        int baseTag = array ? ps.getType().getBaseType().tag() : ps.getType().tag();
        byte typeTag = (byte) (array ? (baseTag | 0x80) : baseTag);

        if (array) {
            int count = ps.count();
            ByteArrayOutputStream elemBuf = new ByteArrayOutputStream();
            DataOutputStream elemOut = new DataOutputStream(elemBuf);
            for (int i = 0; i < count; i++) {
                byte[] elem = encodeScalarForStorage(ps, baseTag, i);
                elemOut.writeInt(elem.length);
                elemOut.write(elem);
            }
            elemOut.flush();
            return new SegmentBuilder.EncodedProperty(nameUtf8, typeTag, count, elemBuf.toByteArray());
        } else {
            byte[] val = encodeScalarForStorage(ps, baseTag, 0);
            return new SegmentBuilder.EncodedProperty(nameUtf8, typeTag, val.length, val);
        }
    }

    /**
     * Encode one scalar value for storage.  For BINARY properties, upload inline
     * blobs to the blob store and store the returned reference as UTF-8.
     * All other types delegate to {@link MerkleHash#encodeScalarValue}.
     */
    private byte[] encodeScalarForStorage(PropertyState ps, int baseTag, int index) throws IOException {
        if (baseTag == BINARY) {
            org.apache.jackrabbit.oak.api.Blob blob = ps.getValue(Type.BINARY, index);
            String ref = blob.getReference();
            if (ref == null || ref.isEmpty()) {
                try {
                    ref = blobStore.putInputStream(blob.getNewStream());
                } catch (BlobAlreadyExistsException e) {
                    ref = e.getRef();
                }
            }
            return ref.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        return MerkleHash.encodeScalarValue(ps, baseTag, index);
    }
}
