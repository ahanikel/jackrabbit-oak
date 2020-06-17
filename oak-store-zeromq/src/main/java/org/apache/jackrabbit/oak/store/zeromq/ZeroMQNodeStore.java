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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.commit.*;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.ServiceScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;

import static org.apache.jackrabbit.oak.store.zeromq.ZeroMQEmptyNodeState.EMPTY_NODE;

/**
 * A store which dumps everything into a queue.
 */
@Component(scope = ServiceScope.SINGLETON, immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
@Service
public class ZeroMQNodeStore implements NodeStore, Observable {

    public static final String PARAM_CLUSTERINSTANCES = "clusterInstances";
    public static final String PARAM_BACKEND_PREFIX = "backendPrefix";
    public static final String PARAM_JOURNAL_PREFIX = "journalPrefix";

    public static String backendPrefix = System.getProperty(PARAM_BACKEND_PREFIX, "localhost");
    public static String journalPrefix = System.getProperty(PARAM_JOURNAL_PREFIX, "localhost");

    private static final Logger log = LoggerFactory.getLogger(ZeroMQNodeStore.class.getName());

    @NotNull
    final ZMQ.Context context;

    private final Integer clusterInstances;

    @NotNull
    final ZMQ.Socket nodeStateReader[];

    @NotNull
    final ZMQ.Socket nodeStateWriter[];

    @NotNull
    final ZMQ.Socket journalReader;

    @NotNull
    final ZMQ.Socket journalWriter;

    @NotNull
    final Cache<String, ZeroMQNodeState> nodeStateCache;

    @NotNull
    final Cache<String, ZeroMQBlob> blobCache;

    private volatile ComponentContext ctx;

    private volatile ChangeDispatcher changeDispatcher;

    @NotNull
    final BlobStore blobStore;

    public ZeroMQNodeStore() {

        context = ZMQ.context(1);

        clusterInstances = Integer.getInteger(PARAM_CLUSTERINSTANCES, 1);

        nodeStateReader = new ZMQ.Socket[clusterInstances];
        nodeStateWriter = new ZMQ.Socket[clusterInstances];

        if ("localhost".equals(backendPrefix)) {
            for (int i = 0; i < clusterInstances; ++i) {
                nodeStateReader[i] = context.socket(ZMQ.REQ);
                nodeStateReader[i].connect("tcp://localhost:" + (8000 + 2*i));

                nodeStateWriter[i] = context.socket(ZMQ.REQ);
                nodeStateWriter[i].connect("tcp://localhost:" + (8001 + 2*i));
            }
        } else {
            for (int i = 0; i < clusterInstances; ++i) {
                nodeStateReader[i] = context.socket(ZMQ.REQ);
                nodeStateReader[i].connect(String.format("tcp://%s%d:8000", backendPrefix, i));

                nodeStateWriter[i] = context.socket(ZMQ.REQ);
                nodeStateWriter[i].connect(String.format("tcp://%s%d:8001", backendPrefix, i));
            }
        }

        journalReader = context.socket(ZMQ.REQ);
        journalReader.connect("tcp://" + journalPrefix + ":9000");

        journalWriter = context.socket(ZMQ.REQ);
        journalWriter.connect("tcp://" + journalPrefix + ":9001");

        nodeStateCache = CacheBuilder.newBuilder()
            .maximumSize(1000).build();

        blobCache = CacheBuilder.newBuilder()
            .maximumSize(100).build();

        blobStore = new FileBlobStore("/tmp/blobs");
    }

    @Activate
    public void activate(ComponentContext ctx) {
        this.ctx = ctx;
        init();
        OsgiWhiteboard whiteboard = new OsgiWhiteboard(ctx.getBundleContext());
        org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean
                ( whiteboard
                , CheckpointMBean.class
                , new ZeroMQCheckpointMBean(this)
                , CheckpointMBean.TYPE
                , "ZeroMQNodeStore checkpoint management"
                , new HashMap<>()
                );
        changeDispatcher = new ChangeDispatcher(getRoot());
    }

    public void init() {
        final String uuid = readRoot();
        if ("undefined".equals(uuid)) {
            reset();
        }
    }

    /**
     * Wipe the complete repo by setting the root to an empty node. The existing
     * nodes remain lingering around unless some GC mechanism (which is not yet
     * implemented) removes them. This is needed for testing.
     */
    public void reset() {
        final NodeBuilder builder = EMPTY_NODE(this, this::readNodeState, this::write).builder();
        builder.setChildNode("root");
        builder.setChildNode("checkpoints");
        builder.setChildNode("blobs");
        NodeState newRoot = builder.getNodeState();
        setRoot(((ZeroMQNodeState) newRoot).getUuid());
    }

    private String readRoot() {
        String msg;
        while (true) {
            try {
                synchronized (journalReader) {
                    journalReader.send("ping");
                    msg = journalReader.recvStr();
                }
                break;
            } catch (Throwable t) {
                log.warn(t.toString());
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                }
            }
        }
        return msg;
    }

    @Override
    public NodeState getRoot() {
        return getSuperRoot().getChildNode("root");
    }

    private synchronized NodeState getSuperRoot() {
        final String uuid = readRoot();
        if ("undefined".equals(uuid)) {
            throw new IllegalStateException("root is undefined, forgot to call init()?");
        }
        return readNodeState(uuid);
    }

    NodeState getBlobRoot() {
        return getSuperRoot().getChildNode("blobs");
    }

    NodeState getCheckpointRoot() {
        return getSuperRoot().getChildNode("checkpoints");
    }

    private void setRoot(String uuid) {
        String msg;
        while (true) {
            try {
                synchronized (journalWriter) {
                    journalWriter.send(uuid);
                    msg = journalWriter.recvStr();
                }
                break;
            } catch (Throwable t) {
                log.warn(t.toString());
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            }
        }
        log.debug(msg);
    }

    private NodeState mergeRoot(String root, NodeState ns) {
        final NodeBuilder rootBuilder = getSuperRoot().builder();
        rootBuilder.setChildNode(root, ns);
        final NodeState newRoot = rootBuilder.getNodeState();
        setRoot(((ZeroMQNodeState) newRoot).getUuid());
        return newRoot;
    }

    @Override
    public synchronized NodeState merge(NodeBuilder builder, CommitHook commitHook, CommitInfo info) throws CommitFailedException {
        if (!(builder instanceof ZeroMQNodeBuilder)) {
            throw new IllegalArgumentException();
        }
        final NodeState newBase = getRoot();
        rebase(builder, newBase);
        final NodeState after = builder.getNodeState();
        final NodeState afterHook = commitHook.processCommit(newBase, after, info);
        mergeRoot("root", afterHook);
        ((ZeroMQNodeBuilder) builder).reset(afterHook);
        if (changeDispatcher != null) {
            changeDispatcher.contentChanged(afterHook, info);
        }
        return afterHook;
    }

    private synchronized NodeState mergeBlob(NodeBuilder builder) {
        if (true) {
            throw new IllegalStateException();
        } else {
            final NodeState newBase = getBlobRoot();
            rebase(builder, newBase);
            final NodeState after = builder.getNodeState();
            mergeRoot("blobs", after);
            ((ZeroMQNodeBuilder) builder).reset(after);
            return after;
        }
    }

    private synchronized NodeState mergeCheckpoint(NodeBuilder builder) {
            final NodeState newBase = getCheckpointRoot();
            rebase(builder, newBase);
            final NodeState after = builder.getNodeState();
            mergeRoot("checkpoints", after);
            ((ZeroMQNodeBuilder) builder).reset(after);
            return after;
    }

    private ZeroMQNodeState readNodeState(String uuid) {
        try {
            return nodeStateCache.get(uuid, () -> {
                final String sNode = read(uuid);
                try {
                    final ZeroMQNodeState ret = ZeroMQNodeState.deSerialise(this, sNode, this::readNodeState, this::write);
                    return ret;
                } catch (ZeroMQNodeState.ParseFailure parseFailure) {
                    if ("Node not found".equals(sNode)) {
                        log.error("Node not found");
                        throw new IllegalStateException("Node not found");
                    } else {
                        log.error(parseFailure.getMessage());
                        throw new IllegalStateException(parseFailure);
                    }
                }
            });
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    private String read(String uuid) {
        String msg;
        int inst = clusterInstanceForUuid(uuid);
        while (true) {
            try {
                synchronized (nodeStateReader[inst]) {
                    nodeStateReader[inst].send(uuid);
                    msg = nodeStateReader[inst].recvStr();
                }
                log.debug("{} read.", uuid);
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
        return msg;
    }

    private void write(ZeroMQNodeState.SerialisedZeroMQNodeState nodeState) {
        String msg;
        int inst = clusterInstanceForUuid(nodeState.getUuid());
        while (true) {
            try {
                synchronized (nodeStateWriter[inst]) {
                    nodeStateWriter[inst].send(nodeState.getserialisedNodeState());
                    msg = nodeStateWriter[inst].recvStr(); // wait for confirmation
                }
                log.debug(msg);
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
    }

    @Override
    public NodeState rebase (@NotNull NodeBuilder builder) {
        final NodeState newBase = getRoot();
        return rebase(builder, newBase);
    }
    public NodeState rebase(@NotNull NodeBuilder builder, NodeState newBase) {
        checkArgument(builder instanceof ZeroMQNodeBuilder);
        NodeState head = checkNotNull(builder).getNodeState();
        NodeState base = builder.getBaseState();
        if (base != newBase) {
            ((ZeroMQNodeBuilder) builder).reset(newBase);
            head.compareAgainstBaseState(
                    base, new ConflictAnnotatingRebaseDiff(builder));
            head = builder.getNodeState();
        }
        return head;
    }

    @Override
    public NodeState reset(NodeBuilder builder) {
        final NodeState newBase = getRoot();
        ((MemoryNodeBuilder) builder).reset(newBase);
        return newBase;
    }

    @Override
    public Blob createBlob(InputStream inputStream) throws IOException {
        if (true) {
            final String ref = blobStore.writeBlob(inputStream);
            return getBlob(ref);
        } else {
            final ZeroMQBlob blob = ZeroMQBlob.newInstance(inputStream);
            blobCache.put(blob.getReference(), blob);
            final NodeBuilder builder = getBlobRoot().builder();
            final String sBlob = blob.serialise();
            final NodeBuilder nBlob = builder.child(blob.getReference());
            nBlob.setProperty("blob", sBlob, Type.STRING);
            mergeBlob(builder);
            return blob;
        }
    }

    Blob createBlob(Blob blob) throws IOException {
        String ref = blob.getReference();
        Blob ret = getBlob(ref);
        if (ref == null || ret == null || ret.getReference() == null) {
            ret = createBlob(blob.getNewStream());
        }
        return ret;
    }

    @Override
    public Blob getBlob(String reference) {
        try {
            if (true) {
                return new AbstractBlob() {
                    @Override
                    public InputStream getNewStream() {
                        try {
                            return blobStore.getInputStream(reference);
                        } catch (IOException ex) {
                            return null;
                        }
                    }

                    @Override
                    public long length() {
                        try {
                            return blobStore.getBlobLength(reference);
                        } catch (IOException ex) {
                            return 0;
                        }
                    }

                    @Override
                    public String getReference() {
                        return reference;
                    }
                };
            } else {
                return blobCache.get(reference, () -> ZeroMQBlob.newInstance(this, reference));
            }
        } catch (ExecutionException e) {
            log.warn("Could not load blob: " + e.toString());
            return null;
        }
    }

    @Override
    public String checkpoint(long lifetimeMicros, Map<String, String> properties) {
        final ZeroMQNodeState currentRoot = (ZeroMQNodeState) getRoot();
        final String nodeName = currentRoot.getUuid().toString() + properties.toString();
        final NodeBuilder builder = getCheckpointRoot().builder()
                .setChildNode(nodeName, currentRoot)
                .setProperty("lifetimeMicros", lifetimeMicros);
        mergeCheckpoint(builder);
        return nodeName;
    }

    @Override
    public String checkpoint(long lifetime) {
        log.error("Unsupported");
        throw new UnsupportedOperationException("checkpoint/1 not supported yet.");
    }

    @Override
    public Map<String, String> checkpointInfo(String checkpoint) {
        log.error("Unsupported");
        throw new UnsupportedOperationException("checkpointInfo/1 not supported yet.");
    }

    @Override
    public Iterable<String> checkpoints() {
        log.error("Unsupported");
        throw new UnsupportedOperationException("checkpoints() not supported yet.");
    }

    @Override
    public NodeState retrieve(String checkpoint) {
        log.error("Unsupported");
        throw new UnsupportedOperationException("retrieve() not supported yet.");
    }

    @Override
    public boolean release(String checkpoint) {
        log.error("Unsupported");
        throw new UnsupportedOperationException("release() not supported yet.");
    }

    @Override
    public Closeable addObserver(Observer observer) {
        return changeDispatcher.addObserver(observer);
    }

    /**
     * Divide the uuid space into @ref{clusterInstances} parts, starting from 0.
     * This implementation limits the number of @ref{clusterInstances} to 2^32-1.
     * If the space cannot be divided equally, the remaining uuids are assigned
     * to the @ref{clusterInstances} - 1 part.
     * @param sUuid
     * @return
     */
    private int clusterInstanceForUuid(String sUuid) {
        return clusterInstanceForUuid(clusterInstances, sUuid);
    }

    static int clusterInstanceForUuid(int clusterInstances, String sUuid) {
        final UUID uuid = UUID.fromString(sUuid);
        final long msb = uuid.getMostSignificantBits();
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
}
