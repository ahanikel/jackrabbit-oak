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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import static javax.jcr.PropertyType.*;

/**
 * Computes SHA-256 Merkle hashes for node records.
 * <p>
 * The hash input is the canonical serialisation of a node's properties and
 * child references (both sorted by name), making the hash structurally
 * deterministic and independent of storage order.
 */
public class MerkleHash {

    /**
     * SHA-256 of empty input — hash of a node with no properties and no children.
     */
    public static final byte[] EMPTY_NODE_HASH = Util.hexToBytes(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");

    /** Null segment identifier: 64 hex zeros. */
    public static final String NULL_HASH_HEX =
            "0000000000000000000000000000000000000000000000000000000000000000";

    /** Null segment identifier as 32 zero bytes. */
    public static final byte[] NULL_HASH_BYTES = new byte[32];

    private static final OutputStream DEV_NULL = new OutputStream() {
        @Override public void write(int b) {}
        @Override public void write(byte[] b, int off, int len) {}
    };

    /**
     * A child entry for Merkle hash computation.
     */
    public static class ChildEntry {
        public final String name;
        public final byte[] merkleHash; // 32 bytes

        public ChildEntry(String name, byte[] merkleHash) {
            if (merkleHash == null || merkleHash.length != 32) {
                throw new IllegalArgumentException("merkleHash must be 32 bytes");
            }
            this.name = name;
            this.merkleHash = merkleHash;
        }
    }

    /**
     * Compute the Merkle hash for a node.
     *
     * @param properties list of properties (will be sorted by name)
     * @param children   list of child entries (will be sorted by name)
     * @return 32-byte SHA-256 hash
     */
    public static byte[] compute(List<? extends PropertyState> properties,
                                 List<ChildEntry> children) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            DigestOutputStream dos = new DigestOutputStream(DEV_NULL, sha256);
            DataOutputStream out = new DataOutputStream(dos);

            // Children sorted by name
            List<ChildEntry> sortedChildren = new ArrayList<>(children);
            sortedChildren.sort((a, b) -> a.name.compareTo(b.name));

            for (ChildEntry child : sortedChildren) {
                byte[] nameBytes = child.name.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                out.writeShort(nameBytes.length);
                out.write(nameBytes);
                out.write(child.merkleHash);
            }

            // Properties sorted by name
            List<PropertyState> sortedProps = new ArrayList<>(properties);
            sortedProps.sort((a, b) -> a.getName().compareTo(b.getName()));

            for (PropertyState ps : sortedProps) {
                byte[] nameBytes = ps.getName().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                out.writeShort(nameBytes.length);
                out.write(nameBytes);
                writePropertyHashInput(out, ps);
            }

            out.flush();
            return sha256.digest();
        } catch (NoSuchAlgorithmException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Write the type_tag + value_length/element_count + value bytes for a property.
     */
    static void writePropertyHashInput(DataOutputStream out, PropertyState ps) throws IOException {
        boolean array = ps.isArray();
        int baseTag = ps.getType().isArray() ? ps.getType().getBaseType().tag() : ps.getType().tag();
        byte typeTag = (byte) (array ? (baseTag | 0x80) : baseTag);
        out.writeByte(typeTag);

        if (array) {
            int count = ps.count();
            out.writeInt(count);
            for (int i = 0; i < count; i++) {
                byte[] elem = encodeScalarValue(ps, baseTag, i);
                out.writeInt(elem.length);
                out.write(elem);
            }
        } else {
            byte[] val = encodeScalarValue(ps, baseTag, 0);
            out.writeInt(val.length);
            out.write(val);
        }
    }

    /**
     * Encode a scalar property value (or one array element) to bytes.
     */
    static byte[] encodeScalarValue(PropertyState ps, int typeTag, int index) {
        switch (typeTag) {
            case STRING:
            case NAME:
            case PATH:
            case DATE:
            case URI:
            case REFERENCE:
            case WEAKREFERENCE: {
                String s = ps.getValue(Type.STRING, index);
                return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
            case LONG: {
                long v = ps.getValue(Type.LONG, index);
                ByteBuffer buf = ByteBuffer.allocate(8);
                buf.putLong(v);
                return buf.array();
            }
            case DOUBLE: {
                double v = ps.getValue(Type.DOUBLE, index);
                ByteBuffer buf = ByteBuffer.allocate(8);
                buf.putDouble(v);
                return buf.array();
            }
            case BOOLEAN: {
                boolean v = ps.getValue(Type.BOOLEAN, index);
                return new byte[]{ v ? (byte) 1 : (byte) 0 };
            }
            case DECIMAL: {
                BigDecimal v = ps.getValue(Type.DECIMAL, index);
                return v.toPlainString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
            case BINARY: {
                // For blobs with a reference, hash the reference bytes.
                // For inline blobs (no reference), hash the raw content bytes.
                org.apache.jackrabbit.oak.api.Blob blob = ps.getValue(Type.BINARY, index);
                String ref = blob.getReference();
                if (ref != null && !ref.isEmpty()) {
                    return ref.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                } else {
                    try {
                        return blob.getNewStream().readAllBytes();
                    } catch (java.io.IOException e) {
                        throw new IllegalStateException("Failed to read inline blob for hashing", e);
                    }
                }
            }
            default:
                throw new IllegalArgumentException("Unknown type tag: " + typeTag);
        }
    }
}
