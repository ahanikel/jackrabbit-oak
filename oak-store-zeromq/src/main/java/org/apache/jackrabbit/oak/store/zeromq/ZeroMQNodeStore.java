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
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

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

    private static final Logger log = LoggerFactory.getLogger(ZeroMQNodeStore.class.getName());

    @NotNull
    final ZContext context;
    private final String instance;

    private int clusterInstances = 1;
    private boolean writeBackJournal = false;
    private boolean writeBackNodes = false;
    private boolean remoteReads = false;
    private boolean logNodeStates = false;
    private String backendPrefix = "localhost";

    @NotNull
    final ZeroMQSocketProvider nodeStateReader[];

    @NotNull
    final ZeroMQSocketProvider nodeStateWriter[];

    @NotNull
    final KVStore<String, ZeroMQNodeState> nodeStateCache;

    @NotNull
    final KVStore<String, ZeroMQBlob> blobCache;

    private volatile ComponentContext ctx;

    private volatile ChangeDispatcher changeDispatcher;

    public final ZeroMQNodeState emptyNode = (ZeroMQNodeState) ZeroMQEmptyNodeState.EMPTY_NODE(this);
    public final ZeroMQNodeState missingNode = (ZeroMQNodeState) ZeroMQEmptyNodeState.MISSING_NODE(this);

    final Object mergeRootMonitor = new Object();
    final Object checkpointMonitor = new Object();
    final Object writeMonitor = new Object();

    ExecutorService nodeWriterThread;
    ExecutorService blobWriterThread;

    private volatile String journalRoot;
    private volatile String checkpointRoot;

    private String initJournal;
    private volatile boolean skip;
    private AtomicLong line = new AtomicLong(0);
    private FileOutputStream nodeStateOutput; // this is only meant for debugging
    private final LoggingHook loggingHook;

    private final GarbageCollectableBlobStore blobStore;

    ZeroMQNodeStore(
            String instance,
            int clusterInstances,
            boolean writeBackJournal,
            boolean writeBackNodes,
            boolean remoteReads,
            String initJournal,
            String backendPrefix,
            boolean logNodeStates,
            String blobCacheDir) {

        this.instance = instance;

        context = new ZContext(50);

        nodeWriterThread = Executors.newFixedThreadPool(50);
        blobWriterThread = Executors.newFixedThreadPool(50); // each thread consumes 1 MB

        this.clusterInstances = clusterInstances;
        this.writeBackJournal = writeBackJournal;
        this.writeBackNodes = writeBackNodes;
        this.remoteReads = remoteReads;
        this.initJournal = initJournal;
        this.logNodeStates = logNodeStates;
        this.backendPrefix = backendPrefix;
        ZeroMQBlob.blobCacheDir = new File(blobCacheDir);

        nodeStateReader = new ZeroMQSocketProvider[clusterInstances];
        nodeStateWriter = new ZeroMQSocketProvider[clusterInstances];

        if ("localhost".equals(backendPrefix)) {
            for (int i = 0; i < clusterInstances; ++i) {
                nodeStateReader[i] = new ZeroMQSocketProvider("tcp://localhost:" + (8000 + 2 * i), context, SocketType.REQ);
                nodeStateWriter[i] = new ZeroMQSocketProvider("tcp://localhost:" + (8001 + 2 * i), context, SocketType.REQ);
            }
        } else {
            for (int i = 0; i < clusterInstances; ++i) {
                nodeStateReader[i] = new ZeroMQSocketProvider(String.format("tcp://%s%d:8000", backendPrefix, i), context, SocketType.REQ);
                nodeStateWriter[i] = new ZeroMQSocketProvider(String.format("tcp://%s%d:8001", backendPrefix, i), context, SocketType.REQ);
            }
        }

        /*
            If we allow remote reads, we just cache the return values
            otherwise we store them in a map
         */
        if (remoteReads) {
            final Cache<String, ZeroMQNodeState> cache =
                    CacheBuilder.newBuilder()
                            .concurrencyLevel(10)
                            .maximumSize(1000000).build();
            nodeStateCache = new NodeStateCache<>(cache, uuid -> {

                final String sNode = read(uuid);
                try {
                    final ZeroMQNodeState ret = ZeroMQNodeState.deSerialise(this, sNode);
                    return ret;
                } catch (ZeroMQNodeState.ParseFailure parseFailure) {
                    if ("Node not found".equals(sNode)) {
                        log.error("Node not found: " + uuid);
                    } else {
                        log.error(parseFailure.getMessage());
                    }
                    return null;
                }
            });
            final Cache<String, ZeroMQBlob> bCache =
                    CacheBuilder.newBuilder()
                            .concurrencyLevel(10)
                            .maximumSize(100000).build();
            blobCache = new NodeStateCache<>(bCache, reference -> {
                try {
                    ZeroMQBlob ret = ZeroMQBlob.newInstance(reference);
                    if (ret == null) {
                        ret = ZeroMQBlob.newInstance(reference, readBlob(reference));
                    }
                    return ret;
                } catch (Throwable t) {
                    log.warn("Could not load blob: " + t.toString());
                    return null;
                }
            });
        } else {
            final Map<String, ZeroMQNodeState> store = new ConcurrentHashMap<>(1000000);
            nodeStateCache = new NodeStateMemoryStore(store);
            final Map<String, ZeroMQBlob> bStore = new ConcurrentHashMap<>(1000000);
            blobCache = new NodeStateMemoryStore<>(bStore);
        }
        if (logNodeStates) {
            try {
                nodeStateOutput = new FileOutputStream(File.createTempFile("nodeStates-", ".log", new File("/tmp")));
            } catch (IOException e) {
            }
        }
        loggingHook = LoggingHook.newLoggingHook(this::write);
        blobStore = new GarbageCollectableBlobStore() {
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


    @Override
    public void close() {
        nodeWriterThread.shutdown();
        blobWriterThread.shutdown();
        for (int i = 0; i < clusterInstances; ++i) {
            nodeStateReader[i].close();
            nodeStateWriter[i].close();
        }
        try {
            if (nodeStateOutput != null) {
                nodeStateOutput.close();
                nodeStateOutput = null;
            }
        } catch (IOException e) {
        }
    }

    @Activate
    public void activate(ComponentContext ctx) {
        this.ctx = ctx;
        init();
        OsgiWhiteboard whiteboard = new OsgiWhiteboard(ctx.getBundleContext());
        org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean
                (whiteboard
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
        props.put("oak.nodestore.description", new String[]{"nodeStoreType=zeromq"});
        //whiteboard.register(NodeStore.class, this, props);
    }

    void init() {
        final String uuid = readRootRemote();
        checkpointRoot = readCPRootRemote();
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
        final NodeState newSuperRoot = builder.getNodeState();
        final ZeroMQNodeState zmqNewSuperRoot = (ZeroMQNodeState) newSuperRoot;
        setRoot(zmqNewSuperRoot.getUuid());
        setCheckpointRoot(emptyNode.getUuid());
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
                nodeStateReader[0].get().send("journal " + instance);
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

    private String readCPRootRemote() {
        if (!remoteReads) {
            return "undefined";
        }

        String msg;
        while (true) {
            try {
                nodeStateReader[0].get().send("checkpoints " + instance);
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

    public ZeroMQNodeState getSuperRoot() {
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
        if ("undefined".equals(checkpointRoot) || checkpointRoot == null) {
            throw new IllegalStateException("checkpointRoot is undefined, forgot to call init()?");
        }
        return readNodeState(checkpointRoot);
    }

    private void setRoot(String uuid) {
        journalRoot = uuid;
        if (writeBackJournal) {
            setRootRemote("journal", uuid);
        }
        /*
        if (writeBackJournal) {
            nodeWriterThread.execute(() -> setRootRemote("journal", uuid));
        }
        */
    }

    private void setCheckpointRoot(String uuid) {
        checkpointRoot = uuid;
        if (writeBackJournal) {
            nodeWriterThread.execute(() -> setRootRemote("checkpoints", uuid));
        }
    }

    private void setRootRemote(String type, String uuid) {
        while (true) {
            synchronized (mergeRootMonitor) {
                try {
                    final ZMQ.Socket socket = nodeStateWriter[0].get();
                    socket.send(type + " " + instance + " " + uuid);
                    socket.recvStr(); // ignore
                    break;
                } catch (Throwable t) {
                    log.warn(t.toString() + " at line " + line.get());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                    }
                }
            }
        }
    }

    private NodeState mergeRoot(String root, NodeState ns) {
        final ZeroMQNodeState superRoot = getSuperRoot();
        final NodeBuilder superRootBuilder = superRoot.builder();
        superRootBuilder.setChildNode(root, ns);
        final ZeroMQNodeState newSuperRoot = (ZeroMQNodeState) superRootBuilder.getNodeState();
        setRoot(newSuperRoot.getUuid());
        return newSuperRoot;
    }

    @Override
    public NodeState merge(NodeBuilder builder, CommitHook commitHook, CommitInfo info) throws CommitFailedException {
        synchronized (mergeRootMonitor) {
            if (!(builder instanceof ZeroMQNodeBuilder)) {
                throw new IllegalArgumentException();
            }
            final NodeState newBase = getRoot();
            // rebase does nothing if the base hasn't changed
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
    }

    private NodeState mergeCheckpoint(NodeBuilder builder) {
        synchronized (checkpointMonitor) {
            final NodeState newBase = getCheckpointRoot();
            rebase(builder, newBase);
            final ZeroMQNodeState after = (ZeroMQNodeState) builder.getNodeState();
            setCheckpointRoot(after.getUuid());
            ((ZeroMQNodeBuilder) builder).reset(after);
            return after;
        }
    }

    @Nullable
    public ZeroMQNodeState readNodeState(String uuid) {
        if (log.isTraceEnabled()) {
            log.trace("{} n? {}", Thread.currentThread().getId(), uuid);
        }
        if (emptyNode.getUuid().equals(uuid)) {
            return emptyNode;
        }
        final ZeroMQNodeState ret = nodeStateCache.get(uuid);
        if (ret == null) {
            log.warn("Node not found: {} ", uuid);
        }
        return ret;
    }

    private String read(String uuid) {
        if (!remoteReads) {
            throw new IllegalStateException("read(uuid) called with remoteReads == false");
        }
        StringBuilder msg;
        final ZMQ.Socket socket = nodeStateReader[0].get();
        while (true) {
            msg = new StringBuilder();
            try {
                socket.send(uuid);
                do {
                    msg.append(socket.recvStr());
                } while (socket.hasReceiveMore());
                if (log.isDebugEnabled()) {
                    log.debug("{} read.", uuid);
                }
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
        if (writeBackNodes) {
            long currentLine = line.incrementAndGet();
            try {
                final ZMQ.Socket writer = nodeStateWriter[0].get();
                writer.send(event);
                log.trace(writer.recvStr()); // ignore
            } catch (Throwable t) {
                log.error(t.getMessage() + " at line " + currentLine);
            }
        }
    }

    void write(ZeroMQNodeState before, ZeroMQNodeState after) {
        final String newUuid = after.getUuid();
        if (nodeStateCache.isCached(newUuid)) {
            return;
        }
        nodeStateCache.put(newUuid, after);
        if (writeBackNodes) {
            synchronized (writeMonitor) {
                loggingHook.processCommit(before, after, null);
            }
        }
        if (nodeStateOutput != null) {
            final String msg = after.getUuid() + "\n" + after.getSerialised() + "\n";
            try {
                nodeStateOutput.write(msg.getBytes());
            } catch (IOException e) {
            }
        }
    }

    private void writeBlob(ZeroMQBlob blob) throws IOException {
        if (!writeBackNodes) {
            return;
        }
        synchronized (writeMonitor) {
            try {
                LoggingHook.writeBlob(blob, this::write);
            } catch (Throwable t) {
                log.error(t.getMessage() + " at line " + line.get());
            }
        }
    }

    private InputStream readBlob(String reference) {
        if (!remoteReads) {
            return null;
        }
        final InputStream ret = new ZeroMQBlobInputStream(nodeStateReader[0], "blob " + reference);
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
        if (blobCache.get(blob.getReference()) == null) {
            writeBlob(blob);
            blobCache.put(blob.getReference(), blob);
        }
        return blob;
    }

    public Blob createBlob(Blob blob) throws IOException {
        if (blob instanceof ZeroMQBlob) {
            blobCache.put(blob.getReference(), (ZeroMQBlob) blob);
            return blob;
        }
        // TODO: remove this?
        if (blob instanceof ZeroMQBlobStoreBlob) {
            return blob;
        }
        String ref = blob.getReference();
        Blob ret = ref == null ? null : getBlob(ref);
        if (ret == null || ret.getReference() == null) {
            ret = createBlob(blob.getNewStream());
            writeBlob((ZeroMQBlob) ret);
        }
        return ret;
    }

    @Override
    public Blob getBlob(String reference) {
        if (reference == null) {
            throw new NullPointerException("reference is null");
        }
        final Blob ret = blobCache.get(reference);
        if (ret == null) {
            final String msg = "Blob " + reference + " not found";
            log.warn(msg);
        }
        return ret;
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
        return blobStore;
    }

    private static interface KVStore<K, V> {
        @Nullable
        public V get(K key);

        public void put(K key, V value);

        public boolean isCached(K key);
    }

    private static class NodeStateMemoryStore<K, V> implements KVStore<K, V> {
        private final Map<K, V> store;

        public NodeStateMemoryStore(Map<K, V> store) {
            this.store = store;
        }

        @Override
        public V get(K key) {
            return store.get(key);
        }

        @Override
        public void put(K key, V value) {
            store.put(key, value);
        }

        @Override
        public boolean isCached(K key) {
            return store.containsKey(key);
        }
    }

    private static class NodeStateCache<K, V> implements KVStore<K, V> {
        private final Cache<K, V> cache;
        private final Function<K, V> remoteReader;

        public NodeStateCache(Cache<K, V> cache, Function<K, V> remoteReader) {
            this.cache = cache;
            this.remoteReader = remoteReader;
        }

        @Override
        public V get(K key) {
            try {
                return cache.get(key, () -> remoteReader.apply(key));
            } catch (ExecutionException e) {
                return null;
            }
        }

        @Override
        public void put(K key, V value) {
            cache.put(key, value);
        }

        @Override
        public boolean isCached(K key) {
            return cache.getIfPresent(key) != null;
        }
    }
}
