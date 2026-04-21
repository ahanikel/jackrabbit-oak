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
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static javax.jcr.PropertyType.*;

/**
 * Immutable {@link NodeState} backed by a binary {@link Segment} record.
 */
public class SegmentNodeState implements NodeState {

    /** Null segment ID: 64 hex zeros — replaces UUID_NULL. */
    public static final String NULL_HASH =
            "0000000000000000000000000000000000000000000000000000000000000000";

    private final SimpleNodeStore store;
    // Null for the sentinel (empty/missing) instances
    private final String segmentId;   // 64-char hex; null for sentinels
    private final int recordIndex;
    private final boolean exists;

    // Lazily loaded
    private volatile Segment.NodeRecord record;

    // -----------------------------------------------------------------------
    // Factory methods
    // -----------------------------------------------------------------------

    public static SegmentNodeState empty(SimpleNodeStore store) {
        return new SegmentNodeState(store, true);
    }

    public static SegmentNodeState missing(SimpleNodeStore store) {
        return new SegmentNodeState(store, false);
    }

    /**
     * Sentinel record index meaning "the root record of the segment"
     * (i.e. {@code nodeCount - 1}).  Used when we know the segment ID
     * but don't yet know how many records it contains.
     */
    public static final int ROOT_RECORD = -1;

    /** Create from a known (segmentId, recordIndex) pair — lazy segment load. */
    public static SegmentNodeState fromRef(SimpleNodeStore store, String segmentId, int recordIndex) {
        return new SegmentNodeState(store, segmentId, recordIndex, null);
    }

    /**
     * Create a reference to the <em>root</em> record of the given segment.
     * The actual record index ({@code nodeCount - 1}) is resolved on first
     * access via {@link #ensureRecord()}.
     */
    public static SegmentNodeState fromRootRef(SimpleNodeStore store, String segmentId) {
        return new SegmentNodeState(store, segmentId, ROOT_RECORD, null);
    }

    /** Create with a pre-loaded record (no lazy load needed). */
    public static SegmentNodeState fromRecord(SimpleNodeStore store, String segmentId,
                                              int recordIndex, Segment.NodeRecord record) {
        return new SegmentNodeState(store, segmentId, recordIndex, record);
    }

    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------

    /** Sentinel constructor (empty/missing). */
    private SegmentNodeState(SimpleNodeStore store, boolean exists) {
        this.store = store;
        this.segmentId = NULL_HASH;
        this.recordIndex = 0;
        this.exists = exists;
        // Sentinel nodes have no properties/children
        this.record = new Segment.NodeRecord(
                MerkleHash.NULL_HASH_BYTES,
                new Segment.ParsedProperty[0],
                new Segment.ParsedChild[0]);
    }

    private SegmentNodeState(SimpleNodeStore store, String segmentId, int recordIndex,
                             Segment.NodeRecord record) {
        this.store = store;
        this.segmentId = segmentId;
        this.recordIndex = recordIndex;
        this.exists = true;
        this.record = record; // may be null → lazy load
    }

    // -----------------------------------------------------------------------
    // Lazy loading
    // -----------------------------------------------------------------------

    private Segment.NodeRecord ensureRecord() {
        if (record == null) {
            synchronized (this) {
                if (record == null) {
                    Segment seg = store.readSegment(segmentId);
                    int idx = (recordIndex == ROOT_RECORD) ? (seg.getNodeCount() - 1) : recordIndex;
                    record = seg.getNodeRecord(idx);
                }
            }
        }
        return record;
    }

    // -----------------------------------------------------------------------
    // Identity / ref
    // -----------------------------------------------------------------------

    /**
     * Returns the 64-char hex Merkle hash of this node.
     * For sentinels this returns {@link #NULL_HASH}.
     */
    public String getRef() {
        if (NULL_HASH.equals(segmentId)) {
            return NULL_HASH;
        }
        return Util.bytesToHex(ensureRecord().merkleHash);
    }

    public SimpleNodeStore getStore() {
        return store;
    }

    public String getSegmentId() {
        return segmentId;
    }

    public int getRecordIndex() {
        return recordIndex;
    }

    // -----------------------------------------------------------------------
    // NodeState interface
    // -----------------------------------------------------------------------

    @Override
    public boolean exists() {
        return exists;
    }

    @Override
    public boolean hasProperty(@NotNull String name) {
        for (Segment.ParsedProperty p : ensureRecord().properties) {
            if (p.name.equals(name)) return true;
        }
        return false;
    }

    @Override
    public @Nullable PropertyState getProperty(@NotNull String name) {
        for (Segment.ParsedProperty p : ensureRecord().properties) {
            if (p.name.equals(name)) {
                return parsedToPropertyState(p);
            }
        }
        return null;
    }

    @Override
    public boolean getBoolean(@NotNull String name) {
        PropertyState ps = getProperty(name);
        return ps != null && ps.getType() == Type.BOOLEAN && ps.getValue(Type.BOOLEAN);
    }

    @Override
    public long getLong(String name) {
        PropertyState ps = getProperty(name);
        if (ps != null && ps.getType() == Type.LONG) {
            return ps.getValue(Type.LONG);
        }
        return 0;
    }

    @Override
    public @Nullable String getString(String name) {
        PropertyState ps = getProperty(name);
        // Follow Oak's AbstractNodeState contract: return string representation of
        // any scalar property, not just those stored with type STRING.
        if (ps != null && !ps.isArray()) {
            return ps.getValue(Type.STRING);
        }
        return null;
    }

    @Override
    public @NotNull Iterable<String> getStrings(@NotNull String name) {
        PropertyState ps = getProperty(name);
        // Follow Oak's AbstractNodeState contract: getValue(STRINGS) handles
        // both scalar STRING (wraps in singleton list) and multi-valued STRINGS.
        if (ps != null) {
            return ps.getValue(Type.STRINGS);
        }
        return Collections.emptyList();
    }

    @Override
    public @Nullable String getName(@NotNull String name) {
        PropertyState ps = getProperty(name);
        // Return name representation of any scalar property, as AbstractNodeState does.
        if (ps != null && !ps.isArray()) {
            return ps.getValue(Type.NAME);
        }
        return null;
    }

    @Override
    public @NotNull Iterable<String> getNames(@NotNull String name) {
        PropertyState ps = getProperty(name);
        // getValue(NAMES) handles both scalar NAME and multi-valued NAMES.
        if (ps != null) {
            return ps.getValue(Type.NAMES);
        }
        return Collections.emptyList();
    }

    @Override
    public long getPropertyCount() {
        return ensureRecord().properties.length;
    }

    @Override
    public @NotNull Iterable<? extends PropertyState> getProperties() {
        Segment.ParsedProperty[] props = ensureRecord().properties;
        List<PropertyState> result = new ArrayList<>(props.length);
        for (Segment.ParsedProperty p : props) {
            result.add(parsedToPropertyState(p));
        }
        return result;
    }

    @Override
    public boolean hasChildNode(@NotNull String name) {
        for (Segment.ParsedChild c : ensureRecord().children) {
            if (c.name.equals(name)) return true;
        }
        return false;
    }

    @Override
    public @NotNull NodeState getChildNode(@NotNull String name) throws IllegalArgumentException {
        for (Segment.ParsedChild c : ensureRecord().children) {
            if (c.name.equals(name)) {
                return resolveChild(c);
            }
        }
        return SegmentNodeState.missing(store);
    }

    @Override
    public long getChildNodeCount(long max) {
        return ensureRecord().children.length;
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        Segment.ParsedChild[] children = ensureRecord().children;
        List<String> names = new ArrayList<>(children.length);
        for (Segment.ParsedChild c : children) {
            names.add(c.name);
        }
        return names;
    }

    @Override
    public @NotNull Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        Segment.ParsedChild[] children = ensureRecord().children;
        List<ChildNodeEntry> entries = new ArrayList<>(children.length);
        for (Segment.ParsedChild c : children) {
            final SegmentNodeState childState = resolveChild(c);
            entries.add(new ChildNodeEntry() {
                @Override public @NotNull String getName() { return c.name; }
                @Override public @NotNull NodeState getNodeState() { return childState; }
            });
        }
        return entries;
    }

    @Override
    public @NotNull NodeBuilder builder() {
        return new SimpleNodeBuilder(this);
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (base instanceof SegmentNodeState) {
            SegmentNodeState sns = (SegmentNodeState) base;
            if (getRef().equals(sns.getRef())) {
                return true;
            }
        }
        return AbstractNodeState.compareAgainstBaseState(this, base, diff);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other instanceof SegmentNodeState) {
            return getRef().equals(((SegmentNodeState) other).getRef());
        }
        if (other instanceof NodeState) {
            return AbstractNodeState.equals(this, (NodeState) other);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getRef().hashCode();
    }

    @Override
    public String toString() {
        return "SegmentNodeState{" + getRef() + "}";
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private SegmentNodeState resolveChild(Segment.ParsedChild c) {
        if (c.external) {
            // Root of the external segment is always at nodeCount-1; use ROOT_RECORD sentinel.
            return SegmentNodeState.fromRootRef(store, c.externalSegmentId);
        } else {
            // Child is in the same segment
            Segment seg = store.readSegment(segmentId);
            Segment.NodeRecord childRecord = seg.getNodeRecord(c.recordIndex);
            return SegmentNodeState.fromRecord(store, segmentId, c.recordIndex, childRecord);
        }
    }

    private PropertyState parsedToPropertyState(Segment.ParsedProperty p) {
        return new SegmentPropertyState(store, p);
    }

    // -----------------------------------------------------------------------
    // Inner class: property state backed by ParsedProperty
    // -----------------------------------------------------------------------

    private static class SegmentPropertyState implements PropertyState {

        private final SimpleNodeStore store;
        private final Segment.ParsedProperty p;

        SegmentPropertyState(SimpleNodeStore store, Segment.ParsedProperty p) {
            this.store = store;
            this.p = p;
        }

        @Override
        public @NotNull String getName() {
            return p.name;
        }

        @Override
        public boolean isArray() {
            return p.isArray;
        }

        @Override
        public Type<?> getType() {
            return typeFromTag(p.typeTag, p.isArray);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> @NotNull T getValue(Type<T> type) {
            if (type.isArray()) {
                List<Object> result = new ArrayList<>();
                for (byte[] elem : p.elements) {
                    result.add(decodeScalar(elem, p.typeTag, type.getBaseType()));
                }
                return (T) Collections.unmodifiableList(result);
            } else {
                return (T) decodeScalar(p.elements[0], p.typeTag, type);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> @NotNull T getValue(Type<T> type, int index) {
            return (T) decodeScalar(p.elements[index], p.typeTag, type);
        }

        @Override
        public long size() {
            return p.elements[0].length;
        }

        @Override
        public long size(int index) {
            return p.elements[index].length;
        }

        @Override
        public int count() {
            return p.isArray ? p.elements.length : 1;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof PropertyState)) return false;
            return org.apache.jackrabbit.oak.plugins.memory.AbstractPropertyState.equal(
                    this, (PropertyState) other);
        }

        @Override
        public int hashCode() {
            return org.apache.jackrabbit.oak.plugins.memory.AbstractPropertyState.hashCode(this);
        }

        private Object decodeScalar(byte[] bytes, int typeTag, Type<?> requestedType) {
            // Handle type conversion: if requestedType is STRING, always return string
            if (requestedType == Type.STRING) {
                return decodeAsString(bytes, typeTag);
            }

            int effectiveTag = requestedType.isArray()
                    ? requestedType.getBaseType().tag()
                    : requestedType.tag();

            switch (effectiveTag) {
                case STRING:
                case NAME:
                case PATH:
                case DATE:
                case URI:
                case REFERENCE:
                case WEAKREFERENCE:
                    return new String(bytes, StandardCharsets.UTF_8);
                case LONG:
                    return ByteBuffer.wrap(bytes).getLong();
                case DOUBLE:
                    return ByteBuffer.wrap(bytes).getDouble();
                case BOOLEAN:
                    return bytes[0] != 0;
                case DECIMAL:
                    return new BigDecimal(new String(bytes, StandardCharsets.UTF_8));
                case BINARY: {
                    String ref = new String(bytes, StandardCharsets.UTF_8).trim();
                    return SimpleBlob.get(store, ref);
                }
                default:
                    return new String(bytes, StandardCharsets.UTF_8);
            }
        }

        private String decodeAsString(byte[] bytes, int typeTag) {
            switch (typeTag) {
                case LONG:
                    return Long.toString(ByteBuffer.wrap(bytes).getLong());
                case DOUBLE:
                    return Double.toString(ByteBuffer.wrap(bytes).getDouble());
                case BOOLEAN:
                    return Boolean.toString(bytes[0] != 0);
                case DECIMAL:
                    return new String(bytes, StandardCharsets.UTF_8);
                case BINARY: {
                    return new String(bytes, StandardCharsets.UTF_8).trim();
                }
                default:
                    return new String(bytes, StandardCharsets.UTF_8);
            }
        }

        private static Type<?> typeFromTag(int typeTag, boolean array) {
            Type<?> base;
            switch (typeTag) {
                case STRING: base = Type.STRING; break;
                case BINARY: base = Type.BINARY; break;
                case LONG: base = Type.LONG; break;
                case DOUBLE: base = Type.DOUBLE; break;
                case DECIMAL: base = Type.DECIMAL; break;
                case DATE: base = Type.DATE; break;
                case BOOLEAN: base = Type.BOOLEAN; break;
                case NAME: base = Type.NAME; break;
                case PATH: base = Type.PATH; break;
                case REFERENCE: base = Type.REFERENCE; break;
                case WEAKREFERENCE: base = Type.WEAKREFERENCE; break;
                case URI: base = Type.URI; break;
                default: base = Type.STRING; break;
            }
            if (!array) return base;
            switch (typeTag) {
                case STRING: return Type.STRINGS;
                case BINARY: return Type.BINARIES;
                case LONG: return Type.LONGS;
                case DOUBLE: return Type.DOUBLES;
                case DECIMAL: return Type.DECIMALS;
                case DATE: return Type.DATES;
                case BOOLEAN: return Type.BOOLEANS;
                case NAME: return Type.NAMES;
                case PATH: return Type.PATHS;
                case REFERENCE: return Type.REFERENCES;
                case WEAKREFERENCE: return Type.WEAKREFERENCES;
                case URI: return Type.URIS;
                default: return Type.STRINGS;
            }
        }
    }
}
