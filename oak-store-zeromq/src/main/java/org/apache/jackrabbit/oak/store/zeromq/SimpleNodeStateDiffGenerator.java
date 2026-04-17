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
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Accumulates node-state diffs and converts the result to a {@link SegmentNodeState}
 * via {@link SegmentWriter}.
 */
public class SimpleNodeStateDiffGenerator implements NodeStateDiff {

    private final SimpleNodeStore store;
    // Maps child name → child SegmentNodeState ref (segment ID)
    private final Map<String, NodeState> childrenMap;
    // Maps property name → PropertyState
    private final Map<String, PropertyState> propertiesMap;

    public SimpleNodeStateDiffGenerator(SegmentNodeState base) {
        this.store = base.getStore();
        this.childrenMap = new HashMap<>();
        this.propertiesMap = new HashMap<>();

        // Initialise from base
        for (String name : base.getChildNodeNames()) {
            childrenMap.put(name, base.getChildNode(name));
        }
        for (PropertyState ps : base.getProperties()) {
            propertiesMap.put(ps.getName(), ps);
        }
    }

    public SegmentNodeState getNodeState() throws IOException {
        // Build a MemoryNodeState-like structure and hand off to SegmentWriter
        org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder builder =
                new org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder(
                        org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE);
        for (Map.Entry<String, PropertyState> e : propertiesMap.entrySet()) {
            builder.setProperty(e.getValue());
        }
        for (Map.Entry<String, NodeState> e : childrenMap.entrySet()) {
            builder.setChildNode(e.getKey(), e.getValue());
        }
        NodeState memNode = builder.getNodeState();

        SegmentWriter writer = new SegmentWriter(store.getRemoteBlobStore());
        String segId = writer.write(memNode);
        byte[] segData = store.getRemoteBlobStore().getBytes(segId);
        Segment seg;
        try {
            seg = Segment.parse(segData);
        } catch (IOException e) {
            throw new IOException("Failed to parse written segment: " + segId, e);
        }
        store.cacheSegment(segId, seg);
        int rootIdx = seg.getNodeCount() - 1;
        return SegmentNodeState.fromRecord(store, segId, rootIdx, seg.getNodeRecord(rootIdx));
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        propertiesMap.put(after.getName(), after);
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        propertiesMap.put(after.getName(), after);
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        propertiesMap.remove(before.getName());
        return true;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        childrenMap.put(name, after);
        return true;
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        childrenMap.put(name, after);
        return true;
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        childrenMap.remove(name);
        return true;
    }
}
