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
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.descriptors.GenericDescriptors;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.framework.Constants;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.ServiceScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo.getOrCreateId;

/**
 * A store which dumps everything into a queue.
 */
@Component(scope = ServiceScope.SINGLETON, immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
@Service
public class ZeroMQNodeStore implements NodeStore, Observable, Closeable {

    public static final String PARAM_CLUSTERINSTANCES = "clusterInstances";
    public static final String PARAM_BACKEND_PREFIX = "backendPrefix";
    public static final String PARAM_WRITEBACKJOURNAL = "writeBackJournal";
    public static final String PARAM_WRITEBACKNODES = "writeBackNodes";
    public static final String PARAM_INITJOURNAL = "initJournal";
    public static final String PARAM_REMOTEREADS = "remoteReads";

    public static String backendPrefix = System.getenv(PARAM_BACKEND_PREFIX);

    private static final Logger log = LoggerFactory.getLogger(ZeroMQNodeStore.class.getName());

    @NotNull
    final ZMQ.Context context;
    private final String instance;

    private boolean initialised;
    private int clusterInstances = 1;
    private boolean writeBackJournal = false;
    private boolean writeBackNodes = false;
    private boolean remoteReads = false;

    @NotNull
    final ZeroMQSocketProvider nodeStateReader[];

    @NotNull
    final ZeroMQSocketProvider nodeStateWriter[];

    @NotNull
    final Cache<String, ZeroMQNodeState> nodeStateCache;

    @NotNull
    final Cache<String, ZeroMQBlob> blobCache;

    private volatile ComponentContext ctx;

    private volatile ChangeDispatcher changeDispatcher;

    public final ZeroMQNodeState emptyNode = (ZeroMQNodeState) ZeroMQEmptyNodeState.EMPTY_NODE(this, this::readNodeState, this::write);
    public final ZeroMQNodeState missingNode = (ZeroMQNodeState) ZeroMQEmptyNodeState.MISSING_NODE(this, this::readNodeState, this::write);

    final Object mergeRootMonitor = new Object();

    ExecutorService nodeWriterThread;
    ExecutorService blobWriterThread;

    private volatile String journalRoot;

    private String initJournal;
    private volatile boolean skip;

    public ZeroMQNodeStore() {
        this("golden");
    }

    public ZeroMQNodeStore(String instance) {

        this.instance = instance;

        initialised = false;

        context = ZMQ.context(20);

        nodeWriterThread = Executors.newFixedThreadPool(5);
        blobWriterThread = Executors.newFixedThreadPool(50); // each thread consumes 1 MB

        try {
            clusterInstances = Integer.parseInt(System.getenv(PARAM_CLUSTERINSTANCES));
            writeBackJournal = Boolean.parseBoolean(System.getenv(PARAM_WRITEBACKJOURNAL));
            writeBackNodes = Boolean.parseBoolean(System.getenv(PARAM_WRITEBACKNODES));
            remoteReads = Boolean.parseBoolean(System.getenv(PARAM_REMOTEREADS));
            initJournal = System.getenv(PARAM_INITJOURNAL);
        } catch (Exception e) {
        }

        nodeStateReader = new ZeroMQSocketProvider[clusterInstances];
        nodeStateWriter = new ZeroMQSocketProvider[clusterInstances];

        if ("localhost".equals(backendPrefix)) {
            for (int i = 0; i < clusterInstances; ++i) {
                nodeStateReader[i] = new ZeroMQSocketProvider("tcp://localhost:" + (8000 + 2*i), context, ZMQ.REQ);
                nodeStateWriter[i] = new ZeroMQSocketProvider("tcp://localhost:" + (8001 + 2*i), context, ZMQ.REQ);
            }
        } else {
            for (int i = 0; i < clusterInstances; ++i) {
                nodeStateReader[i] = new ZeroMQSocketProvider(String.format("tcp://%s%d:8000", backendPrefix, i), context, ZMQ.REQ);
                nodeStateWriter[i] = new ZeroMQSocketProvider(String.format("tcp://%s%d:8001", backendPrefix, i), context, ZMQ.REQ);
            }
        }

        nodeStateCache = CacheBuilder.newBuilder()
            .concurrencyLevel(10)
            .maximumSize(1000000).build();

        blobCache = CacheBuilder.newBuilder()
                .concurrencyLevel(10)
                .maximumSize(100000).build();
    }


    @Override
    public void close() {
        nodeWriterThread.shutdown();
        blobWriterThread.shutdown();
        for (int i = 0; i < clusterInstances; ++i) {
            nodeStateReader[i].close();
            nodeStateWriter[i].close();
        }
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
        WhiteboardExecutor executor = new WhiteboardExecutor();
        executor.start(whiteboard);
        //registerCloseable(executor);
        Map<String, Object> props = new HashMap<>();
        props.put(Constants.SERVICE_PID, ZeroMQNodeStore.class.getName());
        props.put("oak.nodestore.description", new String[]{"nodeStoreType=segment"});
        whiteboard.register(NodeStore.class, this, props);
    }

    public int getClusterInstances() {
        return clusterInstances;
    }

    public void setClusterInstances(int clusterInstances) {
        if (initialised) {
            throw new IllegalAccessError("Cannot be changed after init");
        }
        this.clusterInstances = clusterInstances;
    }

    public boolean doesWriteBackJournal() {
        return writeBackJournal;
    }

    public void setWriteBackJournal(boolean writeBackJournal) {
        if (initialised) {
            throw new IllegalAccessError("Cannot be changed after init");
        }
        this.writeBackJournal = writeBackJournal;
    }

    public boolean doesWriteBackNodes() {
        return writeBackNodes;
    }

    public void setWriteBackNodes(boolean writeBackNodes) {
        if (initialised) {
            throw new IllegalAccessError("Cannot be changed after init");
        }
        this.writeBackNodes = writeBackNodes;
    }

    public boolean doesRemoteReads() {
        return remoteReads;
    }

    public void setRemoteReads(boolean remoteReads) {
        if (initialised) {
            throw new IllegalAccessError("Cannot be changed after init");
        }
        this.remoteReads = remoteReads;
    }

    public void init() {
        initialised = true;
        final String uuid = readRootRemote();
        log.info("Journal root initialised with {}", uuid);
        journalRoot = uuid;
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
        final ZeroMQNodeStateDiffBuilder diff = new ZeroMQNodeStateDiffBuilder(this, emptyNode);
        newSuperRoot.compareAgainstBaseState(emptyNode, diff);
        final ZeroMQNodeState zmqNewSuperRoot = diff.getNodeState();
        final LoggingHook loggingHook = LoggingHook.newLoggingHook(this::write, false);
        loggingHook.processCommit(emptyNode, zmqNewSuperRoot, null);
        setRoot(zmqNewSuperRoot.getUuid());
    }

    public String readRoot() {
        return journalRoot;
    }

    private String readRootRemote() {
        if (initJournal != null) {
            return initJournal;
        }
        if (!remoteReads) {
            return "undefined";
        }

        String msg;
        while (true) {
            try {
                nodeStateReader[0].get().send("journal");
                msg = nodeStateReader[0].get().recvStr();
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

    public NodeState getSuperRoot() {
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

    public void setRoot(String uuid) {
        journalRoot = uuid;
        if (writeBackJournal) {
            nodeWriterThread.execute(() -> setRootRemote(uuid));
        }
    }

    private void setRootRemote(String uuid) {
        while (true) {
            try {
                synchronized (mergeRootMonitor) {
                    final ZMQ.Socket socket = nodeStateWriter[0].get();
                    socket.send("journal " + instance + " " + uuid);
                    socket.recvStr(); // ignore
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
    }

    // this is public because it is not only used by mergeRoot but also by the NodeStateAggregator
    public ZeroMQNodeState mergeSuperRoot(NodeState newSuperRoot, ZeroMQNodeState oldSuperRoot) {
        final ZeroMQNodeStateDiffBuilder diff = new ZeroMQNodeStateDiffBuilder(this, oldSuperRoot);
        newSuperRoot.compareAgainstBaseState(oldSuperRoot, diff);
        ZeroMQNodeState zmqNewSuperRoot = diff.getNodeState();
        setRoot(zmqNewSuperRoot.getUuid());
        return zmqNewSuperRoot;
    }

    public NodeState mergeRoot(String root, NodeState ns) {
        synchronized (mergeRootMonitor) {
            final NodeState superRoot = getSuperRoot();
            final NodeBuilder superRootBuilder = superRoot.builder();
            superRootBuilder.setChildNode(root, ns);
            final NodeState newSuperRoot = superRootBuilder.getNodeState();
            final ZeroMQNodeState zmqNewSuperRoot = mergeSuperRoot(newSuperRoot, (ZeroMQNodeState) superRoot);
            final LoggingHook loggingHook = LoggingHook.newLoggingHook(this::write, false);
            loggingHook.processCommit(superRoot, zmqNewSuperRoot, null);
            return zmqNewSuperRoot;
        }
    }

    @Override
    public NodeState merge(NodeBuilder builder, CommitHook commitHook, CommitInfo info) throws CommitFailedException {
        if (!(builder instanceof ZeroMQNodeBuilder)) {
            throw new IllegalArgumentException();
        }
        final NodeState newBase = getRoot();
        rebase(builder, newBase);
        final NodeState after = builder.getNodeState();
        final NodeState afterHook = commitHook.processCommit(newBase, after, info);
        if (afterHook.equals(newBase)) {
            return newBase; // TODO: There seem to be commits without any changes in them. Need to investigate.
        }
        mergeRoot("root", afterHook);
        ((ZeroMQNodeBuilder) builder).reset(afterHook);
        if (changeDispatcher != null) {
            changeDispatcher.contentChanged(afterHook, info);
        }
        return afterHook;
    }

    private NodeState mergeCheckpoint(NodeBuilder builder) {
        final NodeState newBase = getCheckpointRoot();
        rebase(builder, newBase);
        final NodeState after = builder.getNodeState();
        mergeRoot("checkpoints", after);
        ((ZeroMQNodeBuilder) builder).reset(after);
        return after;
    }

    public ZeroMQNodeState readNodeState(String uuid) {
        try {
            if (log.isTraceEnabled()) {
                log.trace("{} n? {}", Thread.currentThread().getId(), uuid);
            }
            return nodeStateCache.get(uuid, () -> {
                if (emptyNode.getUuid().equals(uuid)) {
                    return emptyNode;
                }
                final String sNode = read(uuid);
                try {
                    final ZeroMQNodeState ret = ZeroMQNodeState.deSerialise(this, sNode, this::readNodeState, this::write);
                    return ret;
                } catch (ZeroMQNodeState.ParseFailure parseFailure) {
                    if ("Node not found".equals(sNode)) {
                        log.error("Node not found: " + uuid);
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
        if (!remoteReads) {
            return "";
        }
        StringBuilder msg;
        final ZMQ.Socket socket = nodeStateReader[0].get();
        while (true) {
            msg = new StringBuilder();
            try {
                socket.send(uuid);
                do {
                    msg.append(socket.recvStr());
                } while(socket.hasReceiveMore());
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
        return msg.toString();
    }

    private void write(String event) {
        if (skip == true) {
            if (event.startsWith("b64!")) {
                skip = false;
            }
            return;
        }
        // TODO:
        // for some reason, this blob (and only this one!) is not written correctly:
        // b64+ 2E79... is ok
        // b64d ... is sent on the quickstart side
        //          but never arrives at the backend
        //          and the quickstart waits forever for a confirmation
        // we ignore the problem for now and just skip this blob
        if (event.contains("2E79040765B71C748E4641664489CAD5")) {
            skip = true;
            return;
        }
        if (writeBackNodes) {
            synchronized (mergeRootMonitor) {
                final ZMQ.Socket writer = nodeStateWriter[0].get();
                writer.send(event);
                log.trace(writer.recvStr()); // ignore
            }
        }
    }

    void write(ZeroMQNodeState nodeState) {
        final String newUuid = nodeState.getUuid();
        if (nodeStateCache.getIfPresent(newUuid) != null) {
            return;
        }
        nodeStateCache.put(newUuid, nodeState);
    }

    private void writeBlob(ZeroMQBlob blob) throws IOException {
        if (!writeBackNodes) {
            return;
        }
        synchronized (mergeRootMonitor) {
            LoggingHook.writeBlob(blob, this::write);
        }
    }

    private InputStream readBlob(String reference) {
        if (!remoteReads) {
            return null;
        }
        final InputStream ret = new ZeroMQBlobInputStream(nodeStateReader[0], reference);
        return ret;
    }

    @Override
    public NodeState rebase(@NotNull NodeBuilder builder) {
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
        final ZeroMQBlob blob = ZeroMQBlob.newInstance(inputStream);
        if (blobCache.getIfPresent(blob.getReference()) == null) {
            writeBlob(blob);
            blobCache.put(blob.getReference(), blob);
        }
        return blob;
    }

    Blob createBlob(Blob blob) throws IOException {
        if (blob instanceof ZeroMQBlob || blob instanceof ZeroMQBlobStoreBlob) {
            return blob;
        }
        String ref = blob.getReference();
        Blob ret = ref == null ? null : getBlob(ref);
        if (ret == null || ret.getReference() == null) {
            ret = createBlob(blob.getNewStream());
        }
        return ret;
    }

    @Override
    public Blob getBlob(String reference) {
        try {
            if (reference == null) {
                throw new NullPointerException("reference is null");
            }
            return blobCache.get(reference, () -> {
                ZeroMQBlob ret = ZeroMQBlob.newInstance(reference, readBlob(reference));
                return ret;
            });
        } catch (ExecutionException e) {
            log.warn("Could not load blob: " + e.toString());
            return null;
        }
    }

    @Override
    public synchronized @NotNull String checkpoint(long lifetime, Map<String, String> properties) {
        long now = System.currentTimeMillis(); // is lifetime millis or micros?

        NodeBuilder checkpoints = getCheckpointRoot().builder();

        for (String n : checkpoints.getChildNodeNames()) {
            NodeBuilder cp = checkpoints.getChildNode(n);
            PropertyState ts = cp.getProperty("timestamp");
            if (ts == null || ts.getType() != LONG || now > ts.getValue(LONG)) {
                cp.remove();
            }
        }

        final ZeroMQNodeState currentRoot = (ZeroMQNodeState) getRoot();
        final String name = UUID.randomUUID().toString();

        NodeBuilder cp = checkpoints.child(name);
        if (Long.MAX_VALUE - now > lifetime) {
            cp.setProperty("timestamp", now + lifetime);
        } else {
            cp.setProperty("timestamp", Long.MAX_VALUE);
        }
        cp.setProperty("created", now);

        NodeBuilder props = cp.setChildNode("properties");
        for (Map.Entry<String, String> p : properties.entrySet()) {
            props.setProperty(p.getKey(), p.getValue());
        }
        cp.setChildNode("root", currentRoot);

        mergeCheckpoint(checkpoints);
        return name;
    }

    @Override
    public synchronized String checkpoint(long lifetime) {
        return checkpoint(lifetime, Collections.<String, String>emptyMap());
    }

    @NotNull
    @Override
    public Map<String, String> checkpointInfo(@NotNull String checkpoint) {
        Map<String, String> properties = newHashMap();
        checkNotNull(checkpoint);
        NodeState cp = getCheckpointRoot()
                .getChildNode(checkpoint)
                .getChildNode("properties");

        for (PropertyState prop : cp.getProperties()) {
            properties.put(prop.getName(), prop.getValue(STRING));
        }

        return properties;
    }

    @Override
    public Iterable<String> checkpoints() {
        final NodeState cpRoot = getCheckpointRoot();
        return cpRoot.getChildNodeNames();
    }

    @Override
    @Nullable
    public NodeState retrieve(@NotNull String checkpoint) {
        checkNotNull(checkpoint);
        final NodeState cpRoot = getCheckpointRoot();
        final NodeState cp = cpRoot.getChildNode(checkpoint).getChildNode("root");
        if (cp.exists()) {
            return cp;
        }
        return null;
    }

    @Override
    public boolean release(@NotNull String checkpoint) {
        checkNotNull(checkpoint);
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

    public GarbageCollectableBlobStore getGarbageCollectableBlobStore() {
        return new GarbageCollectableBlobStore() {
            @Override
            public void setBlockSize(int x) {

            }

            @Override
            public String writeBlob(String tempFileName) throws IOException {
                return createBlob(new FileInputStream(tempFileName)).getReference();
            }

            @Override
            public int sweep() throws IOException {
                return 0;
            }

            @Override
            public void startMark() throws IOException {

            }

            @Override
            public void clearInUse() {

            }

            @Override
            public void clearCache() {

            }

            @Override
            public long getBlockSizeMin() {
                return 0;
            }

            @Override
            public Iterator<String> getAllChunkIds(long maxLastModifiedTime) throws Exception {
                return new Iterator<String>() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public String next() {
                        return null;
                    }
                };
            }

            @Override
            public boolean deleteChunks(List<String> chunkIds, long maxLastModifiedTime) {
                return false;
            }

            @Override
            public long countDeleteChunks(List<String> chunkIds, long maxLastModifiedTime) {
                return 0;
            }

            @Override
            public Iterator<String> resolveChunks(String blobId) {
                return new Iterator<String>() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public String next() {
                        return null;
                    }
                };
            }

            @Override
            public String writeBlob(InputStream in) throws IOException {
                return createBlob(in).getReference();
            }

            @Override
            public String writeBlob(InputStream in, BlobOptions options) throws IOException {
                return writeBlob(in);
            }

            @Override
            public int readBlob(String blobId, long pos, byte[] buff, int off, int length) throws IOException {
                final Blob blob = getBlob(blobId);
                return blob.getNewStream().read(buff, off, length);
            }

            @Override
            public long getBlobLength(String blobId) throws IOException {
                return getBlob(blobId).length();
            }

            @Override
            public InputStream getInputStream(String blobId) throws IOException {
                return getBlob(blobId).getNewStream();
            }

            @Override
            public @Nullable String getBlobId(@NotNull String reference) {
                return reference;
            }

            @Override
            public @Nullable String getReference(@NotNull String blobId) {
                return blobId;
            }

            @Override
            public void close() throws Exception {

            }
        };
    }
}
