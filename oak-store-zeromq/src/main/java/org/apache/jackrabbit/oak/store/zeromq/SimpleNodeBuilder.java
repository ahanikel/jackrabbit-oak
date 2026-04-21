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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class SimpleNodeBuilder extends MemoryNodeBuilder {

    private static final Logger log = LoggerFactory.getLogger(SimpleNodeBuilder.class);

    /**
     * Number of {@link #updated()} calls between in-memory size checks.
     * Kept low enough that we catch large batches early, but high enough
     * that the periodic check itself has negligible cost.
     */
    private static final int CHECK_INTERVAL = 500;

    /**
     * The base state at the start of the <em>current editing session</em>
     * (i.e. since the last {@link #reset} call), preserved across intermediate
     * {@link #flushToSegment()} operations so that the merge pipeline can
     * compute the correct diff even after one or more in-memory flushes.
     * <p>
     * Updated on every {@link #reset} (real commit reset) but NOT by
     * {@link #flushToSegment()}, which calls {@code super.reset()} directly to
     * avoid triggering this field's update.  Non-null only for root builders.
     */
    private NodeState originalBase;

    /**
     * Counter of {@link #updated()} calls since the last size check.
     * Only used by root builders.
     */
    private int updatesSinceCheck = 0;

    /**
     * Set to {@code true} while {@link #flushToSegment()} is running to
     * suppress re-entrant flush attempts (e.g. from {@code super.reset()}).
     */
    private boolean flushing = false;

    /** Cached result of {@link #getNodeState()}; cleared by {@link #updated()}. */
    private SegmentNodeState nodestate = null;

    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------

    /** Root builder, backed by a persisted segment state. */
    public SimpleNodeBuilder(@NotNull NodeState base) {
        super(base);
        assert(base instanceof SegmentNodeState);
        this.originalBase = base;
    }

    /** Child builder, connected to a parent. */
    public SimpleNodeBuilder(SimpleNodeBuilder parent, String name) {
        super(parent, name);
        this.originalBase = null; // child builders don't need this
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Returns the base state that existed <em>before any user edits</em>,
     * even if the builder has been flushed to a segment in the meantime.
     * <p>
     * The merge pipeline uses this to compute the correct diff (what the
     * user changed relative to the repository state at the start of the
     * session), while the internal {@link MemoryNodeBuilder} base always
     * points to the most-recently flushed segment.
     * <p>
     * For child builders this simply delegates to {@link #getBaseState()}.
     */
    public NodeState getOriginalBase() {
        return originalBase != null ? originalBase : getBaseState();
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

    /**
     * Return the current state as an in-memory node state <em>without</em>
     * writing a segment to the remote store. Use this in merge-pipeline
     * internals where the caller will write the final segment itself, to
     * avoid creating unnecessary intermediate segments.
     * <p>
     * If the builder has been flushed to a segment by the high-watermark
     * mechanism, this returns the flushed {@link SegmentNodeState} (possibly
     * with further in-memory changes on top).
     */
    NodeState getMemoryNodeState() {
        // If no dirty changes since the last flush, the cached nodestate
        // already reflects the full current state.
        if (nodestate != null) {
            return nodestate;
        }
        return super.getNodeState();
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
        // Update originalBase so that the next session's diff is computed
        // relative to the newly committed state, not the original pre-session
        // base.  flushToSegment() intentionally calls super.reset() directly
        // to skip this update.
        if (originalBase != null) {
            originalBase = newBase;
        }
        super.reset(newBase);
        nodestate = null;
        updatesSinceCheck = 0;
    }

    // -----------------------------------------------------------------------
    // Change tracking and high-watermark flush
    // -----------------------------------------------------------------------

    @Override
    protected void updated() {
        super.updated();
        nodestate = null;

        // Only the root builder manages the watermark; child updates bubble up
        // via super.updated() and will eventually reach the root.
        if (!isRoot() || originalBase == null || flushing) {
            return;
        }

        updatesSinceCheck++;
        if (updatesSinceCheck >= CHECK_INTERVAL) {
            updatesSinceCheck = 0;
            try {
                checkAndFlush();
            } catch (Exception e) {
                log.warn("High-watermark segment flush failed — continuing with in-memory state", e);
            }
        }
    }

    /**
     * Estimate the in-memory size of the current builder state and flush to a
     * segment if it exceeds {@link SegmentWriter#DEFAULT_MAX_SEGMENT_SIZE}.
     * After flushing, the builder's internal base is replaced with the written
     * {@link SegmentNodeState}, freeing all the in-memory {@code MemoryNodeState}
     * objects. {@link #getOriginalBase()} is unaffected, so the merge pipeline
     * can still compute the correct diff.
     */
    private void checkAndFlush() throws IOException {
        NodeState current = super.getNodeState(); // MemoryNodeState, no segment write
        int estimatedSize = SegmentWriter.estimateInMemorySize(current);
        if (estimatedSize >= SegmentWriter.DEFAULT_MAX_SEGMENT_SIZE) {
            flushToSegment(current);
        }
    }

    /**
     * Write {@code current} to a segment, cache it, and reset the internal
     * {@link MemoryNodeBuilder} base to the written {@link SegmentNodeState}.
     * This frees all in-memory change objects while keeping the builder usable
     * for subsequent edits.
     */
    private void flushToSegment(NodeState current) throws IOException {
        flushing = true;
        try {
            // Use originalBase to obtain the store — it is always a SegmentNodeState.
            SegmentNodeState storeRef = (SegmentNodeState) originalBase;
            SegmentWriter writer = new SegmentWriter(storeRef.getStore().getRemoteBlobStore());
            String segId = writer.write(current);

            byte[] segData = storeRef.getStore().getRemoteBlobStore().getBytes(segId);
            Segment seg = Segment.parse(segData);
            storeRef.getStore().cacheSegment(segId, seg);

            int rootIdx = seg.getNodeCount() - 1;
            SegmentNodeState flushed = SegmentNodeState.fromRecord(
                    storeRef.getStore(), segId, rootIdx, seg.getNodeRecord(rootIdx));

            // Cache so that getNodeState() returns this immediately if no further changes are made.
            nodestate = flushed;
            // Replace the in-memory builder state with the compact segment reference.
            // This frees all MemoryNodeState/MemoryNodeBuilder objects held so far.
            super.reset(flushed);

            log.debug("Flushed builder to segment {} ({} bytes estimated)", segId, SegmentWriter.estimateInMemorySize(current));
        } finally {
            flushing = false;
        }
    }
}
