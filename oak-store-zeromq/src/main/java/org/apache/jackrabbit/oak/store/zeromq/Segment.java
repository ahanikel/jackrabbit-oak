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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Immutable parsed binary segment.
 *
 * <pre>
 * HEADER (13 bytes):
 *   [4] magic 0x4F534B4E
 *   [1] version 1
 *   [4] node_count
 *   [4] ext_ref_count
 *
 * EXTERNAL SEGMENT HASH TABLE:
 *   [ext_ref_count × 32] SHA-256 hashes
 *
 * NODE RECORD OFFSET TABLE:
 *   [node_count × 8]: [4] record_offset [4] record_length
 *
 * DATA SECTION — NODE RECORDS:
 *   [32] merkle_hash
 *   [2]  property_count
 *   [2]  child_count
 *   properties × ([2] name_len [N] name [1] type_tag [4] value_len/count [M] value)
 *   children   × ([2] name_len [N] name [1] ref_type [4|2] ref_data)
 * </pre>
 */
public class Segment {

    public static final int MAGIC = 0x4F534B4E;
    public static final byte VERSION = 1;
    public static final int HEADER_SIZE = 13;

    private final byte[] data;
    private final int nodeCount;
    private final int extRefCount;
    // offsets within data[]
    private final int extRefTableOffset;  // absolute offset of ext ref table
    private final int offsetTableOffset;  // absolute offset of node record offset table
    private final int dataOffset;         // absolute offset of data section

    // Record offsets (relative to dataOffset)
    private final int[] recordOffsets;
    private final int[] recordLengths;

    private Segment(byte[] data, int nodeCount, int extRefCount,
                    int extRefTableOffset, int offsetTableOffset, int dataOffset,
                    int[] recordOffsets, int[] recordLengths) {
        this.data = data;
        this.nodeCount = nodeCount;
        this.extRefCount = extRefCount;
        this.extRefTableOffset = extRefTableOffset;
        this.offsetTableOffset = offsetTableOffset;
        this.dataOffset = dataOffset;
        this.recordOffsets = recordOffsets;
        this.recordLengths = recordLengths;
    }

    public static Segment parse(byte[] data) throws IOException {
        if (data.length < HEADER_SIZE) {
            throw new IOException("Segment too short: " + data.length);
        }
        ByteBuffer buf = ByteBuffer.wrap(data);
        int magic = buf.getInt();
        if (magic != MAGIC) {
            throw new IOException(String.format("Bad segment magic: 0x%08X", magic));
        }
        byte version = buf.get();
        if (version != VERSION) {
            throw new IOException("Unsupported segment version: " + version);
        }
        int nodeCount = buf.getInt();
        int extRefCount = buf.getInt();

        int extRefTableOffset = HEADER_SIZE;
        int offsetTableOffset = extRefTableOffset + extRefCount * 32;
        int dataOffset = offsetTableOffset + nodeCount * 8;

        if (data.length < dataOffset) {
            throw new IOException("Segment truncated");
        }

        int[] recordOffsets = new int[nodeCount];
        int[] recordLengths = new int[nodeCount];
        buf.position(offsetTableOffset);
        for (int i = 0; i < nodeCount; i++) {
            recordOffsets[i] = buf.getInt();
            recordLengths[i] = buf.getInt();
        }

        return new Segment(data, nodeCount, extRefCount,
                extRefTableOffset, offsetTableOffset, dataOffset,
                recordOffsets, recordLengths);
    }

    public int getNodeCount() {
        return nodeCount;
    }

    public int getExtRefCount() {
        return extRefCount;
    }

    public byte[] getRootNodeHash() {
        return getNodeRecord(0).merkleHash;
    }

    public byte[] getExtRef(int index) {
        int off = extRefTableOffset + index * 32;
        return Arrays.copyOfRange(data, off, off + 32);
    }

    public String getExtRefHex(int index) {
        return Util.bytesToHex(getExtRef(index));
    }

    public NodeRecord getNodeRecord(int index) {
        int absOffset = dataOffset + recordOffsets[index];
        ByteBuffer buf = ByteBuffer.wrap(data, absOffset, recordLengths[index]);
        return NodeRecord.parse(buf, this);
    }

    /** Raw bytes of the segment. */
    public byte[] getData() {
        return data;
    }

    /** Parsed, immutable node record. */
    public static class NodeRecord {
        public final byte[] merkleHash;          // 32 bytes
        public final ParsedProperty[] properties;
        public final ParsedChild[] children;

        NodeRecord(byte[] merkleHash, ParsedProperty[] properties, ParsedChild[] children) {
            this.merkleHash = merkleHash;
            this.properties = properties;
            this.children = children;
        }

        static NodeRecord parse(ByteBuffer buf, Segment seg) {
            byte[] hash = new byte[32];
            buf.get(hash);
            int propCount = buf.getShort() & 0xFFFF;
            int childCount = buf.getShort() & 0xFFFF;

            ParsedProperty[] props = new ParsedProperty[propCount];
            for (int i = 0; i < propCount; i++) {
                props[i] = ParsedProperty.parse(buf);
            }

            ParsedChild[] children = new ParsedChild[childCount];
            for (int i = 0; i < childCount; i++) {
                children[i] = ParsedChild.parse(buf, seg);
            }

            return new NodeRecord(hash, props, children);
        }
    }

    /** A parsed property as stored in the binary segment. */
    public static class ParsedProperty {
        public final String name;
        public final byte typeTag;
        public final boolean isArray;
        public final int valueOrCount;  // value_length or element_count
        public final byte[][] elements; // scalar: elements[0] = value bytes; array: each element

        private ParsedProperty(String name, byte typeTag, boolean isArray,
                               int valueOrCount, byte[][] elements) {
            this.name = name;
            this.typeTag = typeTag;
            this.isArray = isArray;
            this.valueOrCount = valueOrCount;
            this.elements = elements;
        }

        static ParsedProperty parse(ByteBuffer buf) {
            int nameLen = buf.getShort() & 0xFFFF;
            byte[] nameBytes = new byte[nameLen];
            buf.get(nameBytes);
            String name = new String(nameBytes, java.nio.charset.StandardCharsets.UTF_8);

            byte rawTag = buf.get();
            boolean isArray = (rawTag & 0x80) != 0;
            byte typeTag = (byte) (rawTag & 0x7F);

            int valueOrCount = buf.getInt();
            byte[][] elements;
            if (isArray) {
                elements = new byte[valueOrCount][];
                for (int i = 0; i < valueOrCount; i++) {
                    int len = buf.getInt();
                    elements[i] = new byte[len];
                    buf.get(elements[i]);
                }
            } else {
                elements = new byte[1][];
                elements[0] = new byte[valueOrCount];
                buf.get(elements[0]);
            }
            return new ParsedProperty(name, typeTag, isArray, valueOrCount, elements);
        }
    }

    /** A parsed child reference as stored in the binary segment. */
    public static class ParsedChild {
        public final String name;
        public final boolean external;
        /** Local child: record index. External child: -1. */
        public final int recordIndex;
        /** External child: segment ID hex. Local child: null. */
        public final String externalSegmentId;

        private ParsedChild(String name, boolean external, int recordIndex, String externalSegmentId) {
            this.name = name;
            this.external = external;
            this.recordIndex = recordIndex;
            this.externalSegmentId = externalSegmentId;
        }

        static ParsedChild parse(ByteBuffer buf, Segment seg) {
            int nameLen = buf.getShort() & 0xFFFF;
            byte[] nameBytes = new byte[nameLen];
            buf.get(nameBytes);
            String name = new String(nameBytes, java.nio.charset.StandardCharsets.UTF_8);

            byte refType = buf.get();
            if (refType == 0) {
                int recordIndex = buf.getInt();
                return new ParsedChild(name, false, recordIndex, null);
            } else {
                int extRefIndex = buf.getShort() & 0xFFFF;
                String segmentId = seg.getExtRefHex(extRefIndex);
                return new ParsedChild(name, true, -1, segmentId);
            }
        }
    }
}
