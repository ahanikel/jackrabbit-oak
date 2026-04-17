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

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;

public class SimpleNodeBuilder extends MemoryNodeBuilder {

    private SegmentNodeState nodestate = null;

    public SimpleNodeBuilder(@NotNull NodeState base) {
        super(base);
        assert(base instanceof SegmentNodeState);
    }

    public SimpleNodeBuilder(SimpleNodeBuilder parent, String name) {
        super(parent, name);
    }

    @Override
    protected void updated() {
        super.updated();
        nodestate = null;
    }

    @Override
    @NotNull
    public SegmentNodeState getNodeState() {
        if (nodestate != null) {
            return nodestate;
        }
        final SegmentNodeState base = (SegmentNodeState) getBaseState();
        final NodeState after = super.getNodeState();
        try {
            SegmentWriter writer = new SegmentWriter(base.getStore().getRemoteBlobStore());
            String segId = writer.write(after);
            // Cache this segment so readSegment works immediately
            byte[] segData = base.getStore().getRemoteBlobStore().getBytes(segId);
            Segment seg = Segment.parse(segData);
            base.getStore().cacheSegment(segId, seg);
            // Root node is always the last record (post-order DFS)
            int rootIdx = seg.getNodeCount() - 1;
            nodestate = SegmentNodeState.fromRecord(base.getStore(), segId, rootIdx, seg.getNodeRecord(rootIdx));
            return nodestate;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public MemoryNodeBuilder createChildBuilder(String name) {
        return new SimpleNodeBuilder(this, name);
    }

    @Override
    public Blob createBlob(InputStream is) throws IOException {
        final SegmentNodeState base = (SegmentNodeState) getBaseState();
        return base.getStore().createBlob(is);
    }

    @Override
    public void reset(@NotNull NodeState newBase) {
        super.reset(newBase);
        nodestate = null;
    }

    /**
     * Return the current state as an in-memory node state <em>without</em>
     * writing a segment to the remote store. Use this in merge-pipeline
     * internals where the caller will write the final segment itself, to
     * avoid creating unnecessary intermediate segments.
     */
    NodeState getMemoryNodeState() {
        return super.getNodeState();
    }
}
