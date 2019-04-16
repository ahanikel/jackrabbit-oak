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
import java.util.logging.Level;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 * A store used for in-memory operations.
 */
public class ZeroMQStore implements SegmentStoreWithGetters, Revisions {

    private static final Logger log = LoggerFactory.getLogger(ZeroMQStore.class.getName());

    static final int UUID_LEN = 36;

    @NotNull
    final ZMQ.Context context;

    @NotNull
    final ZMQ.Poller items;

    /**
     * read segments to be persisted from this socket
     */
    @NotNull
    final ZMQ.Socket segmentWriterService;

    /**
     * written segments are pushed to these queues where
     * a persistence service picks them up
     */
    @NotNull
    final ZMQ.Socket[] segmentWriters;

    /**
     * the segment reader service serves segments by id
     */
    @NotNull
    final ZMQ.Socket segmentReaderService;

    /**
     * the segment readers make requests to the
     * segment servers and return the segment
     */
    @NotNull
    final ZMQ.Socket[] segmentReaders;

    /**
     * our local segment store which keeps the segments
     * we are responsible for
     */
    @NotNull
    final Cache<SegmentId, ByteBuffer> segmentStore;

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

    // this should not need to be volatile in the end, but for now...
    @NotNull
    volatile RecordId head = RecordId.NULL;

    // for debugging
    volatile boolean dirty = false;

    private Object dirtyLock = new Object();

    private final int clusterInstances;

    private final int clusterInstance;

    // I had to copy the whole constructor from MemoryStore
    // because of the call to revisions.bind(this)
    protected ZeroMQStore() throws IOException {

        clusterInstances = Integer.getInteger("clusterInstances");
        clusterInstance = Integer.getInteger("clusterInstance") - 1;

        tracker = new SegmentTracker(new SegmentIdFactory() {
            @Override
            @NotNull
            public SegmentId newSegmentId(long msb, long lsb) {
                return new SegmentId(ZeroMQStore.this, msb, lsb);
            }
        });

        context = ZMQ.context(1);

        segmentWriterService = context.socket(ZMQ.REP);
        segmentWriterService.bind("tcp://localhost:" + (8000 + 2 * clusterInstance));

        segmentWriters = new ZMQ.Socket[clusterInstances];
        for (int i = 0; i < clusterInstances; ++i) {
            if (i == clusterInstance) {
                continue;
            }
            segmentWriters[i] = context.socket(ZMQ.REQ);
            segmentWriters[i].connect("tcp://localhost:" + (8000 + 2 * i));
        }

        segmentReaderService = context.socket(ZMQ.REP);
        segmentReaderService.bind("tcp://localhost:" + (8001 + 2 * clusterInstance));

        segmentReaders = new ZMQ.Socket[clusterInstances];
        for (int i = 0; i < clusterInstances; ++i) {
            if (i == clusterInstance) {
                continue;
            }
            segmentReaders[i] = context.socket(ZMQ.REQ);
            segmentReaders[i].connect("tcp://localhost:" + (8001 + 2 * i));
        }

        segmentStore = CacheBuilder.newBuilder().build();

        segmentCache = CacheBuilder.newBuilder()
            .maximumSize(1000).build();

        items = context.poller(2);
        items.register(segmentReaderService, ZMQ.Poller.POLLIN);
        items.register(segmentWriterService, ZMQ.Poller.POLLIN);

        socketHandler = new Thread("ZeroMQStore Socket Handler") {
            @Override
            public void run() {
                while (!isInterrupted()) {
                    try {
                        items.poll();
                        if (items.pollin(0)) {
                            handleSegmentReaderService(segmentReaderService.recv(0));
                        }
                        if (items.pollin(1)) {
                            handleSegmentWriterService(segmentWriterService.recv(0));
                        }
                    } catch (Throwable t) {
                        log.info(t.toString());
                    }
                }
            }
        };

        segmentReader = new CachingSegmentReader(this::getWriter, null, 16, 2, NoopStats.INSTANCE);
        segmentWriter = defaultSegmentWriterBuilder("sys").withWriterPool().build(this);
    }

    @NotNull
    public static ZeroMQStore newZeroMQStore() throws IOException {
        final ZeroMQStore zmqStore = new ZeroMQStore();
        zmqStore.socketHandler.start();
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("root", EMPTY_NODE);
        final RecordId head = zmqStore.segmentWriter.writeNode(builder.getNodeState());
        zmqStore.setHead(RecordId.NULL, head);
        zmqStore.segmentWriter.flush();
        return zmqStore;
    }

    @Override
    public synchronized boolean containsSegment(SegmentId id) {
        return true;
    }

    @Override
    @NotNull
    public synchronized Segment readSegment(SegmentId id) {
        Segment segment = null;
        final int reader = clusterInstanceForSegmentId(id);
        if (reader == clusterInstance) {
            final ByteBuffer buffer = segmentStore.getIfPresent(id);
            if (buffer != null) {
                segment = new Segment(tracker, segmentReader, id, buffer);
                id.loaded(segment);
                return segment;
            }
        } else {
            //try {
            //segment = segmentCache.get(id, () -> ZeroMQStore.this.readSegmentRemote(reader, id));
            segment = readSegmentRemote(reader, id);
            //} catch (ExecutionException ex) {
            //}
            if (segment != null) {
                return segment;
            }
        }
        throw new SegmentNotFoundException(id);
    }

    private int clusterInstanceForSegmentId(SegmentId id) {

        final long msb = id.getMostSignificantBits();
        final long msbMsb = 2 ^ 32 & (msb >> 32);
        final long inst = msbMsb / (2L ^ 32 / clusterInstances);
        if (inst < 0) {
            throw new IllegalStateException("inst < 0");
        }
        if (inst > clusterInstances) {
            throw new IllegalStateException("inst > clusterInstances");
        }
        return inst == clusterInstances ? clusterInstances - 1 : (int) inst;
    }

    /**
     * read a segment by requesting it via zmq
     */
    private Segment readSegmentRemote(int reader, SegmentId id) {
        log.info("Remotely reading segment {}", id.toString());
        byte[] bytes = null;
        synchronized (segmentReaders[reader]) {
            segmentReaders[reader].send(id.toString());
            bytes = segmentReaders[reader].recv();
            if ("Segment not found".equals(new String(bytes))) {
                throw new SegmentNotFoundException(id);
            }
        }
        return new Segment(getSegmentIdProvider(), getReader(), id, ByteBuffer.wrap(bytes));
    }

    @Override
    public synchronized void writeSegment(
        SegmentId id, byte[] data, int offset, int length) throws IOException {
        final int writer = clusterInstanceForSegmentId(id);
        final ByteBuffer buffer;
        if (id.isDataSegmentId()) {
            if (writer == clusterInstance) {
                if (offset > 4096) {
                    buffer = ByteBuffer.allocate(length);
                    buffer.put(data, offset, length);
                    buffer.rewind();
                } else {
                    buffer = ByteBuffer.wrap(data, offset, length).slice();
                }
                segmentStore.put(id, buffer);
            } else {
                log.info("Remotely writing segment {}", id.toString());
                byte[] bId = id.toString().getBytes();
                assert (UUID_LEN == bId.length);
                final int bufferLength = UUID_LEN + length;
                buffer = ByteBuffer.allocate(bufferLength);
                buffer.put(bId);
                buffer.put(data, offset, length);
                buffer.rewind();
                synchronized (segmentWriters[writer]) {
                    segmentWriters[writer].send(buffer.array(), buffer.arrayOffset(), bufferLength, 0);
                    byte[] msg = segmentWriters[writer].recv(); // wait for confirmation
                    log.info(new String(msg));
                }
                //segmentCache.put(id, segment);
            }
        } else {
            if (writer == clusterInstance) {
                buffer = ByteBuffer.wrap(data, offset, length);
                segmentStore.put(id, buffer);
            }
        }
    }

    synchronized void handleSegmentReaderService(byte[] msg) {
        final String sId = new String(msg, 0, UUID_LEN);
        final UUID uId = UUID.fromString(sId);
        final SegmentId id = tracker.newSegmentId(uId.getMostSignificantBits(), uId.getLeastSignificantBits());
        final int reader = clusterInstanceForSegmentId(id);
        if (reader == clusterInstance) {
            final ByteBuffer buffer = segmentStore.getIfPresent(id);
            if (buffer != null) {
                buffer.rewind();
                segmentReaderService.send(buffer.array(), buffer.arrayOffset(), buffer.remaining(), 0);
            } else {
                segmentReaderService.send("Segment not found");
            }
        } else {
            log.warn("Received request for a segment which is not ours: {}", id.toString());
        }
    }

    synchronized void handleSegmentWriterService(byte[] msg) {
        final String sId = new String(msg, 0, UUID_LEN);
        final UUID uId = UUID.fromString(sId);
        final SegmentId id = tracker.newSegmentId(uId.getMostSignificantBits(), uId.getLeastSignificantBits());
        final int writer = clusterInstanceForSegmentId(id);
        if (writer == clusterInstance) {
            final ByteBuffer segmentBytes = ByteBuffer.wrap(msg, UUID_LEN, msg.length - UUID_LEN);
            segmentStore.put(id, segmentBytes);
            segmentWriterService.send(sId + " confirmed.");
            log.info("Received our segment {}", id.toString());
        } else {
            log.warn("Received segment which is not ours: {}", id.toString());
        }
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
    public synchronized RecordId getHead() {
        if (dirty) {
            waitForDirty();
        }
        return head;
    }

    private void waitForDirty() {
        final long start = System.currentTimeMillis();
        while (dirty) {
            try {
                Thread.sleep(1L);
            } catch (InterruptedException ex) {
            }
        }
        final long end = System.currentTimeMillis();
        log.info("Waited for {} ms", end - start);
    }

    @Override
    public synchronized RecordId getPersistedHead() {
        if (dirty) {
            waitForDirty();
        }
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
            log.error("setHead failed");
            return false;
        }
    }

    @Override
    public synchronized RecordId setHead(Function<RecordId, RecordId> newHead, Option... options) throws InterruptedException {
        // this method throws in MemoryStoreRevisions as well
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

    synchronized void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public void close() throws IOException {
        socketHandler.interrupt();
        items.close();
        segmentWriterService.close();
        segmentReaderService.close();
        for (int i = 0; i < clusterInstances; ++i) {
            if (i != clusterInstance) {
                segmentWriters[i].close();
                segmentReaders[i].close();
            }
        }
        context.close();
    }
}
