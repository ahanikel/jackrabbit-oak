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
package org.apache.jackrabbit.oak.segment.memory;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.commons.io.output.ByteArrayOutputStream;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import org.apache.jackrabbit.oak.segment.CachingSegmentReader;
import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentIdFactory;
import org.apache.jackrabbit.oak.segment.SegmentIdProvider;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentTracker;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.NoopStats;
import org.jetbrains.annotations.NotNull;
import org.zeromq.ZMQ;

/**
 * A store used for in-memory operations.
 */
public class ZeroMQStore implements SegmentStoreWithGetters, Revisions {

    static final int UUID_LEN = 20;

    /**
     * read segments to be persisted from this socket
     */
    @NotNull
    final ZMQ.Socket segmentWriterSocket;

    /**
     * written segments are pushed to this queue where
     * a persistence service picks them up
     */
    @NotNull
    final ZMQ.Socket segmentWriteQueue;

    /**
     * read changes of the root from this socket
     */
    @NotNull
    final ZMQ.Socket rootWriterSocket;

    /**
     * the current root node is written to this queue
     */
    @NotNull
    final ZMQ.Socket rootWriteQueue;

    /**
     * the segment server serves segments by id
     */
    @NotNull
    final ZMQ.Socket segmentServer;

    /**
     * the segment client makes requests to the
     * segment server and returns the segment
     */
    @NotNull
    final ZMQ.Socket segmentClient;

    /**
     * our local segment store which keeps the segments
     * we are responsible for
     */
    @NotNull
    final Cache<SegmentId, Segment> segmentStore;

    /**
     * our segment cache which keeps foreign segments for
     * a while until we evict them
     */
    @NotNull
    final Cache<SegmentId, Segment> segmentCache;

    /**
     * the thread which listens on the sockets and processes messages
     */
    @NotNull
    final Thread socketHandler;

    @NotNull
    final SegmentTracker tracker;

    @NotNull
    final SegmentReader segmentReader;

    @NotNull
    final SegmentWriter segmentWriter;

    RecordId head = null;

    // I had to copy the whole constructor from MemoryStore
    // because of the call to revisions.bind(this)
    protected ZeroMQStore() throws IOException {
        tracker = new SegmentTracker(new SegmentIdFactory() {
            @Override
            @NotNull
            public SegmentId newSegmentId(long msb, long lsb) {
                return new SegmentId(ZeroMQStore.this, msb, lsb);
            }
        });
        segmentReader = new CachingSegmentReader(this::getWriter, null, 16, 2, NoopStats.INSTANCE);
        segmentWriter = defaultSegmentWriterBuilder("sys").withWriterPool().build(this);

        ZMQ.Context context = ZMQ.context(1);
        // where you get segments you have to persist
        segmentWriterSocket = context.socket(ZMQ.PULL);
        // where you can write a new segment to
        segmentWriteQueue = context.socket(ZMQ.PUSH);
        // where you get changes of the root (head) from
        rootWriterSocket = context.socket(ZMQ.PULL);
        // where you can commit a new head
        rootWriteQueue = context.socket(ZMQ.PUSH);
        // where you answer requests for segments you own
        segmentServer = context.socket(ZMQ.REP);
        // where you request segments you don't have
        segmentClient = context.socket(ZMQ.REQ);
        // where you store segments you own
        segmentStore = CacheBuilder.newBuilder().build();
        // where you cache segments you don't own
        segmentCache = CacheBuilder.newBuilder()
            .maximumSize(1000).build();

        segmentWriteQueue.bind("tcp://localhost:8000");
        rootWriteQueue.bind("tcp://localhost:8001");
        segmentServer.bind("tcp://localhost:8002");
        segmentClient.connect("tcp://localhost:8002");

        socketHandler = new Thread("ZeroMQStore Socket Handler") {
            @Override
            public void run() {
                ZMQ.Poller items = new ZMQ.Poller(3);
                items.register(segmentServer, ZMQ.Poller.POLLIN);
                items.register(segmentWriterSocket, ZMQ.Poller.POLLIN);
                items.register(rootWriterSocket, ZMQ.Poller.POLLIN);
                while (!isInterrupted()) {
                    items.poll();
                    if (items.pollin(0)) {
                        handleSegmentServer(segmentServer.recv(0));
                    }
                    if (items.pollin(1)) {
                        handleSegmentWriterSocket(segmentWriterSocket.recv(0));
                    }
                    if (items.pollin(2)) {
                        handleRootWriterSocket(rootWriterSocket.recv(0));
                    }
                }
            }
        };
    }

    @NotNull
    public static ZeroMQStore newZeroMQStore() throws IOException {
        final ZeroMQStore zmqStore = new ZeroMQStore();
        zmqStore.socketHandler.start();
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("root", EMPTY_NODE);
        zmqStore.head = zmqStore.segmentWriter.writeNode(builder.getNodeState());
        zmqStore.segmentWriter.flush();
        return zmqStore;
    }

    @Override
    public boolean containsSegment(SegmentId id) {
        return true;
    }

    @Override
    @NotNull
    public Segment readSegment(SegmentId id) {
        Segment segment = null;
        // let's always get it remotely for now
        /*
            segment = segmentStore.getIfPresent(id);
            if (segment != null) {
            return segment;
            }
        */
        try {
            segment = segmentCache.get(id, () -> ZeroMQStore.this.readSegmentRemote(id));
        } catch (ExecutionException ex) {
        }
        if (segment != null) {
            return segment;
        }
        throw new SegmentNotFoundException(id);
    }

    /**
     * read a segment by requesting it via zmq
     */
    Segment readSegmentRemote(SegmentId id) {
        SegmentId segmentId = ((SegmentId) id);
        segmentClient.send(((SegmentId) id).toString());
        byte[] bytes = segmentClient.recv();
        return new Segment(getSegmentIdProvider(), getReader(), segmentId, ByteBuffer.wrap(bytes));
    }

    @Override
    public void writeSegment(
        SegmentId id, byte[] data, int offset, int length) throws IOException {
        byte[] bId = id.toString().getBytes();
        assert (UUID_LEN == bId.length);
        ByteBuffer buffer = ByteBuffer.allocate(UUID_LEN + length);
        buffer.put(bId);
        buffer.put(data, offset, length);
        buffer.rewind();
        segmentWriteQueue.send(data, offset, length, 0);
    }

    void handleSegmentServer(byte[] msg) {
        final String sId = ByteBuffer.wrap(msg, 0, UUID_LEN).asCharBuffer().toString();
        final UUID uId = UUID.fromString(sId);
        final SegmentId id = new SegmentId(this, uId.getMostSignificantBits(), uId.getLeastSignificantBits());
        final Segment segment = segmentStore.getIfPresent(id);
        if (segment != null) {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try {
                segment.writeTo(bos);
            } catch (IOException ex) {
                // assuming this does not happen
                assert (false);
            }
            segmentServer.send(bos.toByteArray(), 0);
        } else {
            segmentServer.send(SegmentId.NULL.toString().getBytes(), 0);
        }
    }

    void handleSegmentWriterSocket(byte[] msg) {
        final String sId = ByteBuffer.wrap(msg, 0, UUID_LEN).asCharBuffer().toString();
        final UUID uId = UUID.fromString(sId);
        final SegmentId id = new SegmentId(this, uId.getMostSignificantBits(), uId.getLeastSignificantBits());
        final ByteBuffer segmentBytes = ByteBuffer.wrap(msg, UUID_LEN, msg.length - UUID_LEN);
        final Segment segment = new Segment(getSegmentIdProvider(), getReader(), id, segmentBytes);
        segmentStore.put(id, segment);
    }

    void handleRootWriterSocket(byte[] msg) {
        // String sMsg = new String(msg);
    }

    @NotNull
    @Override
    public SegmentWriter getWriter() {
        return segmentWriter;
    }

    @NotNull
    @Override
    public SegmentReader getReader() {
        return segmentReader;
    }

    @NotNull
    @Override
    public SegmentIdProvider getSegmentIdProvider() {
        return tracker;
    }

    @Override
    public RecordId getHead() {
        return head;
    }

    @Override
    public RecordId getPersistedHead() {
        return head;
    }

    @Override
    public synchronized boolean setHead(
        @NotNull RecordId expected,
        @NotNull RecordId head,
        @NotNull Option... options) {
        if (this.head.equals(expected)) {
            this.head = head;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public RecordId setHead(Function<RecordId, RecordId> newHead, Option... options) throws InterruptedException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Revisions getRevisions() {
        return this;
    }

    @Override
    public BlobStore getBlobStore() {
        return null;
    }
}
