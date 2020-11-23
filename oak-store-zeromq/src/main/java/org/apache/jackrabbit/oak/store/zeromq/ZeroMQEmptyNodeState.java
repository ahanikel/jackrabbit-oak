/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.store.zeromq;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.emptyList;

/**
 * Singleton instances of empty and non-existent node states, i.e. ones
 * with neither properties nor child nodes.
 */
public final class ZeroMQEmptyNodeState extends ZeroMQNodeState {

    private final Function<String, ZeroMQNodeState> reader;
    private final Consumer<ZeroMQNodeState> writer;

    public static final NodeState EMPTY_NODE(ZeroMQNodeStore ns, Function<String, ZeroMQNodeState> reader, Consumer<ZeroMQNodeState> writer) {
        return new ZeroMQEmptyNodeState(ns, true, reader, writer);
    }

    public static final NodeState MISSING_NODE(ZeroMQNodeStore ns, Function<String, ZeroMQNodeState> reader, Consumer<ZeroMQNodeState> writer) {
        return new ZeroMQEmptyNodeState(ns, false, reader, writer);
    }

    static final UUID UUID_NULL = new UUID(0L, 0L);

    private final boolean exists;

    private ZeroMQEmptyNodeState(ZeroMQNodeStore ns, boolean exists, Function<String, ZeroMQNodeState> reader, Consumer<ZeroMQNodeState> writer) {
        super(ns, reader, writer);
        this.exists = exists;
        this.reader = reader;
        this.writer = writer;
    }

    @Override
    public boolean exists() {
        return exists;
    }

    @Override
    public long getPropertyCount() {
        return 0;
    }

    @Override
    public boolean hasProperty(@NotNull String name) {
        return false;
    }

    @Override @Nullable
    public PropertyState getProperty(@NotNull String name) {
        return null;
    }

    @Override
    public boolean getBoolean(@NotNull String name) {
        return false;
    }

    @Override
    public long getLong(String name) {
        return 0;
    }

    @Override
    public String getString(String name) {
        return null;
    }

    @NotNull
    @Override
    public Iterable<String> getStrings(@NotNull String name) {
        return emptyList();
    }

    @Override @Nullable
    public String getName(@NotNull String name) {
        return null;
    }

    @Override @NotNull
    public Iterable<String> getNames(@NotNull String name) {
        return emptyList();
    }

    @Override @NotNull
    public Iterable<? extends PropertyState> getProperties() {
        return emptyList();
    }

    @Override
    public long getChildNodeCount(long max) {
        return 0;
    }

    @Override
    public boolean hasChildNode(@NotNull String name) {
        return false;
    }

    @Override @NotNull
    public NodeState getChildNode(@NotNull String name) {
        checkValidName(name);
        return MISSING_NODE(this.ns, reader, writer);
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return emptyList();
    }

    @Override @NotNull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return emptyList();
    }

    @Override @NotNull
    public NodeBuilder builder() {
        return new ZeroMQNodeBuilder(this.ns, this, reader, writer);
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (!isEmptyState(base) && base.exists()) {
            for (PropertyState before : base.getProperties()) {
                if (!diff.propertyDeleted(before)) {
                    return false;
                }
            }
            for (ChildNodeEntry before : base.getChildNodeEntries()) {
                if (!diff.childNodeDeleted(
                        before.getName(), before.getNodeState())) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean compareAgainstEmptyState(
            NodeState state, NodeStateDiff diff) {
        if (!isEmptyState(state) && state.exists()) {
            for (PropertyState after : state.getProperties()) {
                if (!diff.propertyAdded(after)) {
                    return false;
                }
            }
            for (ChildNodeEntry after : state.getChildNodeEntries()) {
                if (!diff.childNodeAdded(after.getName(), after.getNodeState())) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean isEmptyState(NodeState state) {
        return state instanceof ZeroMQEmptyNodeState;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        if (exists) {
            return "{ }";
        } else {
            return "{N/A}";
        }
    }

    public boolean equals(Object object) {
        if (object instanceof ZeroMQEmptyNodeState) {
            return exists == ((ZeroMQEmptyNodeState) object).exists;
        } else if (object instanceof NodeState) {
            NodeState that = (NodeState) object;
            return that.getPropertyCount() == 0
                    && that.getChildNodeCount(1) == 0
                    && (exists == that.exists());
        } else {
            return false;
        }
    }

    public int hashCode() {
        return 0;
    }

}
