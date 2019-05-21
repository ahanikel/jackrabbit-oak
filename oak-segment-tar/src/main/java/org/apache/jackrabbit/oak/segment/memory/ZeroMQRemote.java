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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.NoopStats;
import org.jetbrains.annotations.NotNull;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 * A store used for in-memory operations.
 */
@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class ZeroMQRemote implements SegmentStoreWithGetters, Revisions {

    private static final Logger log = LoggerFactory.getLogger(ZeroMQRemote.class.getName());

    static final int UUID_LEN = 36;

    @NotNull
    final ZMQ.Context context;

    /**
     * written segments are pushed to these queues where
     * a persistence service picks them up
     */
    @NotNull
    final ZMQ.Socket[] segmentWriters;

    /**
     * the segment readers make requests to the
     * segment servers and return the segment
     */
    @NotNull
    final ZMQ.Socket[] segmentReaders;

    /**
     * Protect the head state
     * This is perhaps not necessary and having a lock
     * on the reader or writer socket for the duration of
     * the request may be enough.
     */
    private final Object headMonitor = new Object();

    /**
     * read the current head recordid from this socket
     */
    @NotNull
    final ZMQ.Socket journalReader;

    /**
     * write the new head recordid to this socket
     */
    @NotNull
    final ZMQ.Socket journalWriter;

    /**
     * our segment cache which keeps foreign segments for
     * a while until we evict them
     */
    @NotNull
    final Cache<SegmentId, Segment> segmentCache;

    /**
     * this map will keep segments as long as they are not persisted
     */
    @NotNull
    final Map<String, Segment> unpersistedSegments;

    @NotNull
    final SegmentTracker tracker;

    @NotNull
    final SegmentReader segmentReader;

    @NotNull
    final SegmentWriter segmentWriter;

    private final int clusterInstances;

    private final Thread flusher;

    public ZeroMQRemote() throws IOException {

        clusterInstances = Integer.getInteger("clusterInstances");

        tracker = new SegmentTracker(new SegmentIdFactory() {
            @Override
            @NotNull
            public SegmentId newSegmentId(long msb, long lsb) {
                return new SegmentId(ZeroMQRemote.this, msb, lsb);
            }
        });

        context = ZMQ.context(1);

        segmentWriters = new ZMQ.Socket[clusterInstances];
        for (int i = 0; i < clusterInstances; ++i) {
            segmentWriters[i] = context.socket(ZMQ.REQ);
            segmentWriters[i].connect("tcp://localhost:" + (8000 + 2 * i));
        }

        segmentReaders = new ZMQ.Socket[clusterInstances];
        for (int i = 0; i < clusterInstances; ++i) {
            segmentReaders[i] = context.socket(ZMQ.REQ);
            segmentReaders[i].connect("tcp://localhost:" + (8001 + 2 * i));
        }

        journalWriter = context.socket(ZMQ.REQ);
        journalWriter.connect("tcp://localhost:9000");

        journalReader = context.socket(ZMQ.REQ);
        journalReader.connect("tcp://localhost:9001");

        segmentCache = CacheBuilder.newBuilder()
            .maximumSize(1000).build();

        unpersistedSegments = new HashMap<String, Segment>();

        flusher = new Thread("ZeroMQStore Flush Handler") {
            @Override
            public void run() {
                while (!isInterrupted()) {
                    try {
                        Thread.sleep(15_000);
                    } catch (InterruptedException ex) {
                    }
                    try {
                        log.info("Flushing writer");
                        Set<String> keys = new HashSet<>();
                        synchronized (unpersistedSegments) {
                            keys.addAll(unpersistedSegments.keySet());
                        }
                        segmentWriter.flush();
                        synchronized (unpersistedSegments) {
                            for (String key : keys) {
                                unpersistedSegments.remove(key);
                            }
                        }
                    } catch (IOException ex) {
                        log.warn(ex.toString());
                    }
                }
            }
        };

        segmentReader = new CachingSegmentReader(this::getWriter, null, 0, 0, NoopStats.INSTANCE);
        segmentWriter = defaultSegmentWriterBuilder("sys").withWriterPool().build(this);

        startBackgroundThreads();
    }

    @NotNull
    public static ZeroMQRemote newZeroMQRemote() throws IOException {
        final ZeroMQRemote zmqStore = new ZeroMQRemote();
        if (zmqStore.getHead().equals(RecordId.NULL)) {
            NodeBuilder builder = EMPTY_NODE.builder();
            builder.setChildNode("root", EMPTY_NODE);
            final RecordId head = zmqStore.segmentWriter.writeNode(builder.getNodeState());
            zmqStore.setHead(RecordId.NULL, head);
        }
        return zmqStore;
    }

    @Override
    public boolean containsSegment(SegmentId id) {
        if (id.sameStore(this)) {
            return true;
        }
        try {
            readSegment(id);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    @Override
    @NotNull
    public Segment readSegment(SegmentId id) {
        Segment segment;
        synchronized (unpersistedSegments) {
            segment = unpersistedSegments.getOrDefault(id.toString(), null);
        }
        if (segment == null) {
            final int reader = clusterInstanceForSegmentId(id);
            try {
                segment = segmentCache.getIfPresent(id);
                if (segment == null) {
                    segment = readSegmentRemote(reader, id);
                }
            } catch (IllegalArgumentException ex) {
                // this is fatal, just catching for debugging/documentation
                throw ex;
            }
        }
        if (segment == null) {
            throw new SegmentNotFoundException(id);
        }
        return segment;
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
        byte[] bytes;
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
            final Buffer buffer = Buffer.allocate(bytes.length);
            buffer.put(bytes);
            buffer.rewind();
            segment = new Segment(getSegmentIdProvider(), getReader(), id, buffer);
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
        Buffer buffer;
        buffer = Buffer.allocate(length);
        buffer.put(data, offset, length);
        buffer.rewind();
        final Segment segment = new Segment(getSegmentIdProvider(), getReader(), id, buffer);
        segmentCache.put(id, segment);

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
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    log.error(e.toString());
                }
            }
        }
        notifySegmentPersisted(id.toString());
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
            if (!getHead().equals(expected)) {
                log.error("setHead failed");
                return false;
            }
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
            try {
                segmentWriter.flush();
            } catch (IOException e) {
                log.error(e.toString());
                return false;
            }
            return true;
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

    @Override
    public void close() throws IOException {
        stopBackgroundThreads();
        segmentWriter.flush();
        for (int i = 0; i < clusterInstances; ++i) {
            segmentWriters[i].close();
            segmentReaders[i].close();
        }
        journalWriter.close();
        journalReader.close();
        context.close();
    }

    private void startBackgroundThreads() {
        flusher.start();
    }

    private void stopBackgroundThreads() {
        flusher.interrupt();
    }

    @Override
    public void notifyNewSegment(String segmentId, Segment segment) {
        synchronized (unpersistedSegments) {
            unpersistedSegments.put(segmentId, segment);
            log.info("Unpersisted segments: {}", unpersistedSegments.size());
        }
    }

    private void notifySegmentPersisted(String segmentId) {
        synchronized (unpersistedSegments) {
            unpersistedSegments.remove(segmentId);
        }
    }
}
