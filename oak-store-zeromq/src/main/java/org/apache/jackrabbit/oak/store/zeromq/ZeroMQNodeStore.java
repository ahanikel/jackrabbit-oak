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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.commons.SimpleValueFactory;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.commit.*;
import org.apache.jackrabbit.oak.spi.descriptors.GenericDescriptors;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo.getOrCreateId;
import static org.apache.jackrabbit.oak.store.zeromq.ZeroMQNodeState.getNodeStateDiffBuilder;

/**
 * A store which dumps everything into a queue.
 */
@Component(scope = ServiceScope.SINGLETON, immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
@Service
public class ZeroMQNodeStore implements NodeStore, Observable {

    public static final String PARAM_CLUSTERINSTANCES = "clusterInstances";
    public static final String PARAM_BACKEND_PREFIX = "backendPrefix";
    public static final String PARAM_JOURNAL_PREFIX = "journalPrefix";
    public static final String PARAM_BLOBEND_PREFIX = "blobendPrefix";

    public static String backendPrefix = System.getProperty(PARAM_BACKEND_PREFIX, "localhost");
    public static String journalPrefix = System.getProperty(PARAM_JOURNAL_PREFIX, "localhost");
    public static String blobendPrefix = System.getProperty(PARAM_BLOBEND_PREFIX, "localhost");

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
    final ZMQ.Socket blobReader[];

    @NotNull
    final ZMQ.Socket blobWriter[];

    @NotNull
    final Cache<String, ZeroMQNodeState> nodeStateCache;

    @NotNull
    final Cache<String, ZeroMQBlob> blobCache;

    private volatile ComponentContext ctx;

    private volatile ChangeDispatcher changeDispatcher;

    final ZeroMQNodeState emptyNode = (ZeroMQNodeState) ZeroMQEmptyNodeState.EMPTY_NODE(this, this::readNodeState, this::write);
    final ZeroMQNodeState missingNode = (ZeroMQNodeState) ZeroMQEmptyNodeState.MISSING_NODE(this, this::readNodeState, this::write);

    final Object mergeRootMonitor = new Object();
    final Object mergeMonitor = new Object();
    final Object mergeBlobMonitor = new Object();
    final Object mergeCheckpointMonitor = new Object();

    final ExecutorService nodeWriterThread = Executors.newFixedThreadPool(5);
    final ExecutorService blobWriterThread = Executors.newFixedThreadPool(5); // each thread consumes 100 MB

    public ZeroMQNodeStore() {

        context = ZMQ.context(1);

        clusterInstances = Integer.getInteger(PARAM_CLUSTERINSTANCES, 1);

        nodeStateReader = new ZMQ.Socket[clusterInstances];
        nodeStateWriter = new ZMQ.Socket[clusterInstances];
        blobReader = new ZMQ.Socket[clusterInstances];
        blobWriter = new ZMQ.Socket[clusterInstances];

        if ("localhost".equals(backendPrefix)) {
            for (int i = 0; i < clusterInstances; ++i) {
                nodeStateReader[i] = context.socket(ZMQ.REQ);
                nodeStateReader[i].connect("tcp://localhost:" + (8000 + 2*i));

                nodeStateWriter[i] = context.socket(ZMQ.REQ);
                nodeStateWriter[i].connect("tcp://localhost:" + (8001 + 2*i));

                blobReader[i] = context.socket(ZMQ.REQ);
                blobReader[i].connect("tcp://localhost:" + (10000 + 2*i));

                blobWriter[i] = context.socket(ZMQ.REQ);
                blobWriter[i].connect("tcp://localhost:" + (10001 + 2*i));
            }
        } else {
            for (int i = 0; i < clusterInstances; ++i) {
                nodeStateReader[i] = context.socket(ZMQ.REQ);
                nodeStateReader[i].connect(String.format("tcp://%s%d:8000", backendPrefix, i));

                nodeStateWriter[i] = context.socket(ZMQ.REQ);
                nodeStateWriter[i].connect(String.format("tcp://%s%d:8001", backendPrefix, i));

                blobReader[i] = context.socket(ZMQ.REQ);
                blobReader[i].connect(String.format("tcp://%s%d:10000", blobendPrefix, i));

                blobWriter[i] = context.socket(ZMQ.REQ);
                blobWriter[i].connect(String.format("tcp://%s%d:10001", blobendPrefix, i));
            }
        }

        journalReader = context.socket(ZMQ.REQ);
        journalReader.connect("tcp://" + journalPrefix + ":9000");

        journalWriter = context.socket(ZMQ.REQ);
        journalWriter.connect("tcp://" + journalPrefix + ":9001");

        nodeStateCache = CacheBuilder.newBuilder()
            .concurrencyLevel(10)
            .maximumSize(1000000).build();

        blobCache = CacheBuilder.newBuilder()
            .concurrencyLevel(10)
            .maximumSize(100000).build();
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
            // ensure a clusterId is initialized
            // and expose it as 'oak.clusterid' repository descriptor
            GenericDescriptors clusterIdDesc = new GenericDescriptors();
            clusterIdDesc.put(
                    ClusterRepositoryInfo.OAK_CLUSTERID_REPOSITORY_DESCRIPTOR_KEY,
                    new SimpleValueFactory().createValue(getOrCreateId(this)),
                    true,
                    false
            );
            whiteboard.register(Descriptors.class, clusterIdDesc, new HashMap<>());
            // Register "discovery lite" descriptors
            whiteboard.register(Descriptors.class, new ZeroMQDiscoveryLiteDescriptors(this), new HashMap<>());
    }

    public void init() {
        final String uuid = readRoot();
        if ("undefined".equals(uuid)) {
            reset();
        }
        changeDispatcher = new ChangeDispatcher(getRoot());
    }

    /**
     * Wipe the complete repo by setting the root to an empty node. The existing
     * nodes remain lingering around unless some GC mechanism (which is not yet
     * implemented) removes them. This is needed for testing.
     */
    public void reset() {
        final NodeBuilder builder = emptyNode.builder();
        builder.setChildNode("root");
        builder.setChildNode("checkpoints");
        builder.setChildNode("blobs");
        NodeState newSuperRoot = builder.getNodeState();
        // this may seem strange but is needed because newSuperRoot is a MemoryNodeState
        // and we need a ZeroMQNodeState
        final ZeroMQNodeState.ZeroMQNodeStateDiffBuilder diff = getNodeStateDiffBuilder(this, emptyNode, this::readNodeState, this::write);
        newSuperRoot.compareAgainstBaseState(emptyNode, diff);
        final ZeroMQNodeState zmqNewSuperRoot = diff.getNodeState();
        setRoot(zmqNewSuperRoot.getUuid());
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
        synchronized (mergeRootMonitor) {
            final NodeState superRoot = getSuperRoot();
            final NodeBuilder superRootBuilder = superRoot.builder();
            superRootBuilder.setChildNode(root, ns);
            final NodeState newSuperRoot = superRootBuilder.getNodeState();
            final ZeroMQNodeState.ZeroMQNodeStateDiffBuilder diff = getNodeStateDiffBuilder(this, (ZeroMQNodeState) superRoot, this::readNodeState, this::write);
            newSuperRoot.compareAgainstBaseState(superRoot, diff);
            final ZeroMQNodeState zmqNewSuperRoot = diff.getNodeState();
            setRoot(zmqNewSuperRoot.getUuid());
            return newSuperRoot;
        }
    }

    @Override
    public NodeState merge(NodeBuilder builder, CommitHook commitHook, CommitInfo info) throws CommitFailedException {
        if (!(builder instanceof ZeroMQNodeBuilder)) {
            throw new IllegalArgumentException();
        }
        synchronized (mergeMonitor) {
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
    }

    private NodeState mergeBlob(NodeBuilder builder) {
        if (true) {
            throw new IllegalStateException();
        } else {
            synchronized (mergeBlobMonitor) {
                final NodeState newBase = getBlobRoot();
                rebase(builder, newBase);
                final NodeState after = builder.getNodeState();
                mergeRoot("blobs", after);
                ((ZeroMQNodeBuilder) builder).reset(after);
                return after;
            }
        }
    }

    private NodeState mergeCheckpoint(NodeBuilder builder) {
        synchronized (mergeCheckpointMonitor) {
            final NodeState newBase = getCheckpointRoot();
            rebase(builder, newBase);
            final NodeState after = builder.getNodeState();
            mergeRoot("checkpoints", after);
            ((ZeroMQNodeBuilder) builder).reset(after);
            return after;
        }
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
        final String uuid = nodeState.getUuid();
        if (nodeStateCache.getIfPresent(uuid) != null) {
            return;
        }
        nodeStateCache.put(uuid, nodeState.getNodeState());
        nodeWriterThread.execute(() -> {
            String msg;
            int inst = clusterInstanceForUuid(uuid);
            while (true) {
                try {
                    synchronized (nodeStateWriter[inst]) {
                        nodeStateWriter[inst].send(uuid + "\n" + nodeState.getserialisedNodeState());
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
        });
    }

    private void writeBlob(ZeroMQBlob blob) {
        blobWriterThread.execute(() -> {
            final InputStream is = blob.getStringStream();
            final String reference = blob.getReference();
            String msg;
            int inst = clusterInstanceForBlobId(reference);
            final byte[] buffer = new byte[1024 * 1024 * 100]; // 100 MB
            while (true) {
                try {
                    synchronized (blobWriter[inst]) {
                        blobWriter[inst].sendMore(reference + "\n");
                        for (int nRead = is.read(buffer); nRead > 0; nRead = is.read(buffer)) {
                            if (nRead < buffer.length) {
                                blobWriter[inst].sendMore(Arrays.copyOfRange(buffer, 0, nRead));
                            } else {
                                blobWriter[inst].sendMore(buffer);
                            }
                        }
                        blobWriter[inst].send("");
                        msg = blobWriter[inst].recvStr(); // wait for confirmation
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
        });
    }

    private ZeroMQBlob readBlob(String reference) {
        String msg;
        final int inst = clusterInstanceForBlobId(reference);
        synchronized (blobReader[inst]) {
            while (true) {
                try {
                    blobReader[inst].send(reference);
                    final InputStream is = new InputStream() {
                        final byte[] buffer = new byte[1024 * 1024 * 100]; // 100 MB
                        volatile int cur = 0;
                        volatile int max = blobReader[inst].recv(buffer, 0, buffer.length, 0);

                        @Override
                        public int read() {
                            if (cur == max) {
                                nextBunch();
                            }
                            if (max < 1) {
                                return -1;
                            }
                            return buffer[cur++];
                        }

                        private void nextBunch() {
                            synchronized (blobReader[inst]) {
                                max = blobReader[inst].recv(buffer, 0, buffer.length, 0);
                                cur = 0;
                            }
                        }
                    };
                    return ZeroMQBlob.newInstance(is);
                } catch (Throwable t) {
                    log.warn("Failed to load blob, retrying: {}", t.getMessage());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                    }
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
            final ZeroMQBlob blob = ZeroMQBlob.newInstance(inputStream);
            if (blobCache.getIfPresent(blob.getReference()) == null) {
                blobCache.put(blob.getReference(), blob);
            }
            writeBlob(blob);
            return blob;
        } else {
            final ZeroMQBlob blob = ZeroMQBlob.newInstance(inputStream);
            if (blobCache.getIfPresent(blob.getReference()) == null) {
                blobCache.put(blob.getReference(), blob);
            }
            blobWriterThread.execute(new Thread("BlobStore Writer Thread") {
                public void run() {
                    try {
                        final NodeBuilder builder = getBlobRoot().builder();
                        final String sBlob = blob.serialise();
                        final NodeBuilder nBlob = builder.child(blob.getReference());
                        nBlob.setProperty("blob", sBlob, Type.STRING);
                        mergeBlob(builder);
                    } catch (Throwable t) {
                        log.error("Error occurred while saving blob", t);
                    }
                }
            });
            return blob;
        }
    }

    Blob createBlob(Blob blob) throws IOException {
        if (blob instanceof ZeroMQBlob || blob instanceof  ZeroMQBlobStoreBlob) {
            return blob;
        }
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
                return blobCache.get(reference, () -> {
                    ZeroMQBlob ret = ZeroMQBlob.newInstance(reference);
                    if (ret == null) {
                        ret = readBlob(reference);
                    }
                    return ret;
                });
        } catch (ExecutionException e) {
            log.warn("Could not load blob: " + e.toString());
            return null;
        }
    }

    @Override
    public String checkpoint(long lifetimeMicros, Map<String, String> properties) {
        final ZeroMQNodeState currentRoot = (ZeroMQNodeState) getRoot();
        final String nodeName = currentRoot.getUuid().toString() + properties.toString();
        final NodeBuilder builder = getCheckpointRoot().builder();
        final NodeBuilder cpBuilder = builder.child(nodeName);
        cpBuilder.setChildNode("root", currentRoot);
        cpBuilder.setProperty("lifetimeMicros", lifetimeMicros);
        properties.forEach(cpBuilder.child("props")::setProperty);
        mergeCheckpoint(builder);
        return nodeName;
    }

    @Override
    public String checkpoint(long lifetime) {
        return checkpoint(lifetime, new HashMap<>());
    }

    @Override
    public Map<String, String> checkpointInfo(String checkpoint) {
        // TODO: parse the checkpoint name, no need to read the repo
        final NodeState cpRoot = getCheckpointRoot();
        final NodeState cpNode = cpRoot.getChildNode(checkpoint).getChildNode("props");
        Map<String, String> ret = new HashMap<>();
        cpNode.getProperties().forEach(ps -> ret.put(ps.getName(), ps.getValue(Type.STRING)));
        return ret;
    }

    @Override
    public Iterable<String> checkpoints() {
        final NodeState cpRoot = getCheckpointRoot();
        return cpRoot.getChildNodeNames();
    }

    @Override
    public NodeState retrieve(String checkpoint) {
        final NodeState cpRoot = getCheckpointRoot();
        final NodeState cp = cpRoot.getChildNode(checkpoint);
        if (cp.exists()) {
            return cp.getChildNode("root");
        } else {
            return null;
        }
    }

    @Override
    public boolean release(String checkpoint) {
        final NodeBuilder cpRoot = getCheckpointRoot().builder();
        boolean ret = cpRoot.getChildNode(checkpoint).remove();
        if (ret) {
            mergeCheckpoint(cpRoot);
        }
        return ret;
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

    private int clusterInstanceForBlobId(String id) {
        // parse first 4 bytes = 32 bits
        final long msb = Long.parseLong(id.substring(0, 8), 16);
        final long msbMsb = msb & 0xffff_ffffL;
        final long inst = msbMsb / (0x1_0000_0000L / clusterInstances);
        if (inst < 0) {
            throw new IllegalStateException("inst < 0");
        }
        if (inst > clusterInstances) {
            throw new IllegalStateException("inst > clusterInstances");
        }
        return inst == clusterInstances ? clusterInstances - 1 : (int) inst;    }
}
