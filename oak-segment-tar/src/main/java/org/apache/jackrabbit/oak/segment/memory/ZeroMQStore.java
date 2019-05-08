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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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
import org.apache.jackrabbit.oak.segment.spi.persistence.Buffer;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.NoopStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 * A store used for in-memory operations.
 */
@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class ZeroMQStore implements SegmentStoreWithGetters, Revisions {

    private static final Logger log = LoggerFactory.getLogger(ZeroMQStore.class.getName());

    static final int UUID_LEN = 36;

    @NotNull
    final ZMQ.Context context;

    @NotNull
    final ZMQ.Poller pollerItems;

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

    @NotNull
    final ZMQ.Socket journalReader;

    @NotNull
    final ZMQ.Socket journalWriter;

    /**
     * our local segment store which keeps the segments
     * we are responsible for
     */
    @NotNull
    final Cache<SegmentId, Buffer> segmentStore;

    /**
     * our segment cache which keeps foreign segments for
     * a while until we evict them
     */
    @NotNull
    final Cache<SegmentId, Segment> segmentCache;

    /**
     * this cache will keep segments as long as they are not persisted
     */
    @NotNull
    final Map<String, Segment> unpersistedSegments;

    /**
     * the thread which listens on the sockets and processes messages
     */
    @Nullable
    private final Thread socketHandler;

    @NotNull
    final SegmentTracker tracker;

    @NotNull
    final SegmentReader segmentReader;

    @NotNull
    final SegmentWriter segmentWriter;

    private final int clusterInstances;

    private final int clusterInstance;

    private final boolean remoteOnly;

    @NotNull
    private final BlobStore blobStore;

    private final Object headMonitor = new Object();

    // I had to copy the whole constructor from MemoryStore
    // because of the call to revisions.bind(this)
    public ZeroMQStore() throws IOException {

        clusterInstances = Integer.getInteger("clusterInstances");
        clusterInstance = Integer.getInteger("clusterInstance") - 1;
        remoteOnly = clusterInstance == -1;

        tracker = new SegmentTracker(new SegmentIdFactory() {
            @Override
            @NotNull
            public SegmentId newSegmentId(long msb, long lsb) {
                return new SegmentId(ZeroMQStore.this, msb, lsb);
            }
        });

        context = ZMQ.context(1);

        segmentWriterService = context.socket(ZMQ.REP);
        if (!remoteOnly) {
            segmentWriterService.bind("tcp://localhost:" + (8000 + 2 * clusterInstance));
        }

        segmentWriters = new ZMQ.Socket[clusterInstances];
        for (int i = 0; i < clusterInstances; ++i) {
            if (i == clusterInstance) {
                continue;
            }
            segmentWriters[i] = context.socket(ZMQ.REQ);
            segmentWriters[i].connect("tcp://localhost:" + (8000 + 2 * i));
        }

        segmentReaderService = context.socket(ZMQ.REP);
        if (!remoteOnly) {
            segmentReaderService.bind("tcp://localhost:" + (8001 + 2 * clusterInstance));
        }

        segmentReaders = new ZMQ.Socket[clusterInstances];
        for (int i = 0; i < clusterInstances; ++i) {
            if (i == clusterInstance) {
                continue;
            }
            segmentReaders[i] = context.socket(ZMQ.REQ);
            segmentReaders[i].connect("tcp://localhost:" + (8001 + 2 * i));
        }

        journalWriter = context.socket(ZMQ.REQ);
        journalWriter.connect("tcp://localhost:9000");

        journalReader = context.socket(ZMQ.REQ);
        journalReader.connect("tcp://localhost:9001");

        segmentStore = CacheBuilder.newBuilder().build();

        segmentCache = CacheBuilder.newBuilder()
            .maximumSize(100).build();

        unpersistedSegments = new HashMap<String, Segment>();

        pollerItems = context.poller(2);
        if (remoteOnly) {
            socketHandler = null;
        } else {
            pollerItems.register(segmentReaderService, ZMQ.Poller.POLLIN);
            pollerItems.register(segmentWriterService, ZMQ.Poller.POLLIN);

            socketHandler = new Thread("ZeroMQStore Socket Handler") {
                @Override
                public void run() {
                    while (!isInterrupted()) {
                        try {
                            pollerItems.poll();
                            if (pollerItems.pollin(0)) {
                                handleSegmentReaderService(segmentReaderService.recv(0));
                            }
                            if (pollerItems.pollin(1)) {
                                handleSegmentWriterService(segmentWriterService.recv(0));
                            }
                        } catch (Throwable t) {
                            log.error(t.toString());
                        }
                    }
                }
            };
        }

        blobStore = new MemoryBlobStore();
        segmentReader = new CachingSegmentReader(this::getWriter, blobStore, 16, 2, NoopStats.INSTANCE);
        segmentWriter = defaultSegmentWriterBuilder("sys").withWriterPool().build(this);

        if (!remoteOnly) {
            startBackgroundThreads();
        }
    }

    @NotNull
    public static ZeroMQStore newZeroMQStore() throws IOException {
        final ZeroMQStore zmqStore = new ZeroMQStore();
        if (zmqStore.remoteOnly) {
            NodeBuilder builder = EMPTY_NODE.builder();
            builder.setChildNode("root", EMPTY_NODE);
            final RecordId head = zmqStore.segmentWriter.writeNode(builder.getNodeState());
            zmqStore.setHead(RecordId.NULL, head);
            zmqStore.segmentWriter.flush();
        }
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
        final int reader = clusterInstanceForSegmentId(id);
        if (reader == clusterInstance) {
            final Buffer buffer = segmentStore.getIfPresent(id);
            if (buffer != null) {
                segment = new Segment(tracker, segmentReader, id, buffer);
                notifySegmentPersisted(id.toString());
                return segment;
            }
        } else {
            try {
                segment = segmentCache.getIfPresent(id);
                if (segment == null) {
                    synchronized (unpersistedSegments) {
                        segment = unpersistedSegments.get(id.toString());
                    }
                    if (segment == null) {
                        segment = readSegmentRemote(reader, id);
                    }
                }
            } catch (IllegalArgumentException ex) {
                throw ex;
            }
            if (segment != null) {
                return segment;
            }
        }
        throw new SegmentNotFoundException(id);
    }

    private int clusterInstanceForSegmentId(SegmentId id) {

        final long msb = id.getMostSignificantBits();
        final long msbMsb = 0xffff_ffffL & (msb >> 32);
        final long inst = msbMsb / (0x1_0000_0000L / clusterInstances);
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
        final String sId = id.toString();
        log.info("Remotely reading segment {}", sId);
        byte[] bytes = null;
        // if any network errors occur, retry until we succeed
        while (true) {
            try {
                synchronized (segmentReaders[reader]) {
                    segmentReaders[reader].send(sId);
                    bytes = segmentReaders[reader].recv();
                }
                break;
            } catch (Throwable t) {
                try {
                    log.warn(t.toString());
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                }
            }
        }
        if ("Segment not found".equals(new String(bytes))) {
            return null;
        }
        final Segment segment;
        try {
            segment = new Segment(getSegmentIdProvider(), getReader(), id, Buffer.wrap(bytes));
        } catch (IllegalArgumentException ex) {
            // this is fatal, just catching for debugging/documentation
            throw ex;
        }
        segmentCache.put(id, segment);
        notifySegmentPersisted(sId);
        return segment;
    }

    @Override
    public void writeSegment(
        SegmentId id, byte[] data, int offset, int length) throws IOException {
        final int writer = clusterInstanceForSegmentId(id);
        final Buffer buffer;
        try {
            if (writer == clusterInstance) {
                buffer = Buffer.wrap(data, offset, length);
                segmentStore.put(id, buffer);
            } else {
                log.info("Remotely writing segment {}", id.toString());
                final byte[] bId = id.toString().getBytes();
                assert (UUID_LEN == bId.length);
                final int bufferLength = UUID_LEN + length;
                buffer = Buffer.allocate(bufferLength);
                buffer.put(bId);
                buffer.put(data, offset, length);
                buffer.rewind();
                // retry forever if any network errors occur
                while (true) {
                    try {
                        final byte[] msg;
                        synchronized (segmentWriters[writer]) {
                            segmentWriters[writer].send(buffer.array(), buffer.arrayOffset(), bufferLength, 0);
                            msg = segmentWriters[writer].recv(); // wait for confirmation
                        }
                        log.info(new String(msg));
                        break;
                    } catch (Throwable t) {
                        log.warn(t.toString());
                        Thread.sleep(100);
                    }
                }
                final Segment segment = new Segment(getSegmentIdProvider(), getReader(), id, Buffer.wrap(data, offset, length));
                segmentCache.put(id, segment);
                notifySegmentPersisted(id.toString());
            }
        } catch (InterruptedException e) {
            log.error(e.toString());
        } catch (Throwable t) {
            log.error("Unable to write segment: {}", t.toString());
            throw t;
        }
    }

    void handleSegmentReaderService(byte[] msg) {
        final String sId = new String(msg, 0, UUID_LEN);
        final UUID uId = UUID.fromString(sId);
        final SegmentId id = tracker.newSegmentId(uId.getMostSignificantBits(), uId.getLeastSignificantBits());
        final int reader = clusterInstanceForSegmentId(id);
        if (reader == clusterInstance) {
            final Buffer buffer = segmentStore.getIfPresent(id);
            if (buffer != null) {
                //buffer.rewind();
                if (buffer.array()[buffer.arrayOffset()] != '0'
                    || buffer.array()[buffer.arrayOffset() + 1] != 'a'
                    || buffer.array()[buffer.arrayOffset() + 2] != 'K') {
                    log.error("buffer is broken");
                }
                segmentReaderService.send(buffer.array(), buffer.arrayOffset(), buffer.remaining(), 0);
            } else {
                segmentReaderService.send("Segment not found");
                log.error("Requested segment {} not found.", id.toString());
            }
        } else {
            log.error("Received request for a segment which is not ours: {}", id.toString());
        }
    }

    void handleSegmentWriterService(byte[] msg) {
        final String sId = new String(msg, 0, UUID_LEN);
        final UUID uId = UUID.fromString(sId);
        final SegmentId id = tracker.newSegmentId(uId.getMostSignificantBits(), uId.getLeastSignificantBits());
        final int writer = clusterInstanceForSegmentId(id);
        if (writer == clusterInstance) {
            final Buffer segmentBytes = Buffer.wrap(msg, UUID_LEN, msg.length - UUID_LEN).slice();
            segmentStore.put(id, segmentBytes);
            segmentWriterService.send(sId + " confirmed.");
            log.debug("Received our segment {}", id.toString());
        } else {
            log.error("Received segment which is not ours: {}", id.toString());
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
    public RecordId getHead() {
        byte[] msg;
        synchronized (headMonitor) {
            while (true) {
                try {
                    journalReader.send("ping");
                    msg = journalReader.recv();
                    break;
                } catch (Throwable t) {
                    log.warn(t.toString());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ex) {
                    }
                }
            }
            final String sMsg = new String(msg);
            final RecordId head = RecordId.fromString(tracker, sMsg);
            return head;
        }
    }

    @Override
    public RecordId getPersistedHead() {
        return getHead();
    }

    @Override
    public boolean setHead(
        @NotNull RecordId expected,
        @NotNull RecordId head,
        @NotNull Option... options) {

        final String sMsg = head.toString();
        final byte[] msg = sMsg.getBytes();

        synchronized (headMonitor) {
            if (getHead().equals(expected)) {
                while (true) {
                    try {
                        journalWriter.send(msg);
                        final byte[] resp = journalWriter.recv();
                        log.info(new String(resp));
                        break;
                    } catch (Throwable t) {
                        log.warn(t.toString());
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ex) {
                        }
                    }
                }
                return true;
            } else {
                log.error("setHead failed");
                return false;
            }
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
        return blobStore;
    }

    @Override
    public void close() throws IOException {
        stopBackgroundThreads();
        pollerItems.close();
        segmentWriterService.close();
        segmentReaderService.close();
        for (int i = 0; i < clusterInstances; ++i) {
            if (i != clusterInstance) {
                segmentWriters[i].close();
                segmentReaders[i].close();
            }
        }
        journalWriter.close();
        journalReader.close();
        context.close();
    }

    private void startBackgroundThreads() {
        if (socketHandler != null) {
            socketHandler.start();
        }
    }

    private void stopBackgroundThreads() {
        if (socketHandler != null) {
            socketHandler.interrupt();
        }
    }

    @Override
    public boolean isRemoteOnly() {
        return remoteOnly;
    }

    @Override
    public void notifyNewSegment(String segmentId, Segment segment) {
        if (remoteOnly) {
            synchronized (unpersistedSegments) {
                unpersistedSegments.put(segmentId, segment);
            }
            log.debug("(Adding) Unpersisted segments: {}", unpersistedSegments.size());
        }
    }

    private void notifySegmentPersisted(String segmentId) {
        if (remoteOnly) {
            synchronized (unpersistedSegments) {
                unpersistedSegments.remove(segmentId);
            }
            final StringBuffer buf = new StringBuffer();
            unpersistedSegments.forEach((id, segment) -> buf.append(id).append(", "));
            if (buf.length() > 1) {
                log.info("(Removing) Unpersisted segments: {}", buf.delete(buf.length() - 2, buf.length() - 1).toString());
            }
        }
    }
}
