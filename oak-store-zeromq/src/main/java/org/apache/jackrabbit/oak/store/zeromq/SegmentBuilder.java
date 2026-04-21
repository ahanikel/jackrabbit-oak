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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Mutable accumulator that builds a binary segment.
 * Call {@link #addNodeRecord} to add nodes (post-order DFS),
 * then {@link #flush} to get the final byte array.
 */
public class SegmentBuilder {

    /** Pending node record entry. */
    private static class PendingRecord {
        final byte[] merkleHash;          // 32 bytes
        final List<EncodedProperty> props;
        final List<PendingChild> children;

        PendingRecord(byte[] merkleHash, List<EncodedProperty> props, List<PendingChild> children) {
            this.merkleHash = merkleHash;
            this.props = props;
            this.children = children;
        }
    }

    /** A local or external child reference pending in this segment. */
    static class PendingChild {
        final String name;
        final boolean external;
        final int localRecordIndex;   // for local refs
        final byte[] extSegHashBytes; // for external refs: 32 bytes

        /** Local child. */
        PendingChild(String name, int recordIndex) {
            this.name = name;
            this.external = false;
            this.localRecordIndex = recordIndex;
            this.extSegHashBytes = null;
        }

        /** External child. */
        PendingChild(String name, byte[] extSegHashBytes) {
            this.name = name;
            this.external = true;
            this.localRecordIndex = -1;
            this.extSegHashBytes = extSegHashBytes;
        }
    }

    /** A binary-encoded property (without name prefix — name is stored separately). */
    static class EncodedProperty {
        final byte[] nameUtf8;
        final byte typeTag;      // raw tag (includes 0x80 for arrays)
        final int valueOrCount;  // value_length for scalars, element_count for arrays
        final byte[] valueBytes; // for scalars: raw value; for arrays: cat of [4-byte len + elem_bytes]

        EncodedProperty(byte[] nameUtf8, byte typeTag, int valueOrCount, byte[] valueBytes) {
            this.nameUtf8 = nameUtf8;
            this.typeTag = typeTag;
            this.valueOrCount = valueOrCount;
            this.valueBytes = valueBytes;
        }

        /** Serialised size in the segment: 2 + nameLen + 1 + 4 + valueBytes.length */
        int serialisedSize() {
            return 2 + nameUtf8.length + 1 + 4 + valueBytes.length;
        }
    }

    private final List<PendingRecord> records = new ArrayList<>();
    // ext ref table: hash-hex → index, preserving insertion order
    private final LinkedHashMap<String, Integer> extRefs = new LinkedHashMap<>();

    private int currentDataSize = 0;

    /**
     * Add a node record.
     *
     * @param merkleHash 32-byte Merkle hash
     * @param props      encoded properties
     * @param children   child refs
     * @return record index (0-based)
     */
    public int addNodeRecord(byte[] merkleHash,
                             List<EncodedProperty> props,
                             List<PendingChild> children) {
        // Register external refs
        for (PendingChild child : children) {
            if (child.external) {
                String key = Util.bytesToHex(child.extSegHashBytes);
                extRefs.computeIfAbsent(key, k -> extRefs.size());
            }
        }

        PendingRecord rec = new PendingRecord(merkleHash, props, children);
        currentDataSize += recordDataSize(rec);
        int index = records.size();
        records.add(rec);
        return index;
    }

    /** Estimate the serialised size of a record's data (for the DATA section). */
    private int recordDataSize(PendingRecord rec) {
        int size = 32 + 2 + 2; // merkle_hash + property_count + child_count
        for (EncodedProperty p : rec.props) {
            size += p.serialisedSize();
        }
        for (PendingChild c : rec.children) {
            byte[] nameBytes = c.name.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            size += 2 + nameBytes.length + 1; // name_len + name + ref_type
            size += c.external ? 2 : 4;       // ext_ref_index (short) or record_index (int)
        }
        return size;
    }

    /** Current estimated size of the segment data section (without headers/tables). */
    public int currentDataSize() {
        return currentDataSize;
    }

    /** Current estimated total segment size (including headers and tables). */
    public int currentTotalSize() {
        int n = records.size();
        int e = extRefs.size();
        return Segment.HEADER_SIZE + e * 32 + n * 8 + currentDataSize;
    }

    public int getNodeCount() {
        return records.size();
    }

    /**
     * Serialise all records into a byte array.
     *
     * @return complete binary segment
     */
    public byte[] flush() throws IOException {
        int nodeCount = records.size();
        int extRefCount = extRefs.size();

        // Build ext ref index lookup: hex-string → index
        String[] extRefHexes = new String[extRefCount];
        for (Map.Entry<String, Integer> e : extRefs.entrySet()) {
            extRefHexes[e.getValue()] = e.getKey();
        }

        // First pass: compute record data bytes and their offsets
        byte[][] recordDataBytes = new byte[nodeCount][];
        int[] offsets = new int[nodeCount];
        int offset = 0;
        for (int i = 0; i < nodeCount; i++) {
            offsets[i] = offset;
            recordDataBytes[i] = serialiseRecord(records.get(i));
            offset += recordDataBytes[i].length;
        }

        int totalSize = Segment.HEADER_SIZE + extRefCount * 32 + nodeCount * 8 + offset;
        ByteBuffer buf = ByteBuffer.allocate(totalSize);

        // Header
        buf.putInt(Segment.MAGIC);
        buf.put(Segment.VERSION);
        buf.putInt(nodeCount);
        buf.putInt(extRefCount);

        // External segment hash table
        for (String hex : extRefHexes) {
            buf.put(Util.hexToBytes(hex));
        }

        // Node record offset table
        for (int i = 0; i < nodeCount; i++) {
            buf.putInt(offsets[i]);
            buf.putInt(recordDataBytes[i].length);
        }

        // Data section
        for (byte[] rd : recordDataBytes) {
            buf.put(rd);
        }

        return buf.array();
    }

    private byte[] serialiseRecord(PendingRecord rec) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);

        out.write(rec.merkleHash);
        out.writeShort(rec.props.size());
        out.writeShort(rec.children.size());

        for (EncodedProperty p : rec.props) {
            out.writeShort(p.nameUtf8.length);
            out.write(p.nameUtf8);
            out.writeByte(p.typeTag);
            out.writeInt(p.valueOrCount);
            out.write(p.valueBytes);
        }

        for (PendingChild c : rec.children) {
            byte[] nameBytes = c.name.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.writeShort(nameBytes.length);
            out.write(nameBytes);
            if (c.external) {
                out.writeByte(1);
                String key = Util.bytesToHex(c.extSegHashBytes);
                int extIdx = extRefs.get(key);
                out.writeShort(extIdx);
            } else {
                out.writeByte(0);
                out.writeInt(c.localRecordIndex);
            }
        }

        out.flush();
        return bos.toByteArray();
    }
}
