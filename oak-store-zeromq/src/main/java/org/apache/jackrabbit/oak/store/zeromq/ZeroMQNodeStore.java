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
import org.apache.jackrabbit.oak.spi.commit.ObserverTracker;
import org.apache.jackrabbit.oak.spi.descriptors.GenericDescriptors;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
import org.zeromq.ZSocket;

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
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
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
@Component(
        scope = ServiceScope.SINGLETON,
        immediate = true,
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        property = "oak.nodestore.description=nodeStoreType=zeromq"
)
@Service
public class ZeroMQNodeStore implements NodeStore, Observable, Closeable, GarbageCollectableBlobStore {

    private static final Logger log = LoggerFactory.getLogger(ZeroMQNodeStore.class.getName());

    @NotNull
    ZContext context;
    private String journalId;

    private int clusterInstances;
    private boolean writeBackJournal;
    private boolean writeBackNodes;
    private boolean remoteReads;

    @NotNull
    ZeroMQSocketProvider nodeStateReader[];

    @NotNull
    ZeroMQSocketProvider nodeStateWriter[];

    @NotNull
    KVStore<String, ZeroMQNodeState> nodeStateCache;

    @NotNull
    KVStore<String, ZeroMQBlob> blobCache;

    private volatile ComponentContext ctx;

    private volatile ChangeDispatcher changeDispatcher;

    public final ZeroMQNodeState emptyNode = (ZeroMQNodeState) ZeroMQEmptyNodeState.EMPTY_NODE(this);
    public final ZeroMQNodeState missingNode = (ZeroMQNodeState) ZeroMQEmptyNodeState.MISSING_NODE(this);

    final Object mergeRootMonitor = new Object();
    final Object checkpointMonitor = new Object();
    final Object writeMonitor = new Object();

    private volatile String journalRoot;
    private volatile String checkpointRoot;

    private String initJournal;
    private FileOutputStream nodeStateOutput; // this is only meant for debugging
    private LoggingHook loggingHook;

    private volatile boolean configured;
    private String storeId = UUID.randomUUID().toString();
    private OsgiWhiteboard whiteboard;
    private WhiteboardExecutor executor;
    private ObserverTracker observerTracker;

    private AtomicLong remoteReadNodeCounter = new AtomicLong();
    private AtomicLong remoteWriteNodeCounter = new AtomicLong();
    private AtomicLong remoteReadBlobCounter = new AtomicLong();
    private AtomicLong remoteWriteBlobCounter = new AtomicLong();

    private volatile String journalPending = null;
    private ZMQ.Socket journalEvents;
    private Thread journalEventHandler;

    public ZeroMQNodeStore() {
    }

    ZeroMQNodeStore(ZeroMQNodeStoreBuilder builder) {
        configure(builder.getJournalId(), builder.getClusterInstances(), builder.isWriteBackJournal(), builder.isWriteBackNodes(), builder.isRemoteReads(), builder.getInitJournal(), builder.getBackendPrefix(), builder.isLogNodeStates(), builder.getBlobCacheDir());
    }

    private void configure(
                String journalId,
        int clusterInstances,
        boolean writeBackJournal,
        boolean writeBackNodes,
        boolean remoteReads,
        String initJournal,
        String backendPrefix,
        boolean logNodeStates,
        String blobCacheDir) {

        if (configured) {
            return;
        }

        configured = true;
        this.journalId = journalId;

        context = new ZContext(50);

        this.clusterInstances = clusterInstances;
        this.writeBackJournal = writeBackJournal;
        this.writeBackNodes = writeBackNodes;
        this.remoteReads = remoteReads;
        this.initJournal = initJournal;
        ZeroMQBlob.blobCacheDir = new File(blobCacheDir);
        ZeroMQBlob.blobCacheDir.mkdir();

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

        if (writeBackJournal) {
            journalEvents = context.createSocket(SocketType.SUB);
            if ("localhost".equals(backendPrefix)) {
                journalEvents.connect("tcp://localhost:9000");
            } else {
                journalEvents.connect(String.format("tcp://%s%d:9000", backendPrefix, 0));
            }
            journalEvents.subscribe("journal");
            journalEventHandler = new Thread(() -> {
                while (true) {
                    String msg = journalEvents.recvStr();
                    StringTokenizer msgReader = new StringTokenizer(msg);
                    String op = msgReader.nextToken();
                    String jId = msgReader.nextToken();
                    String newUuid = msgReader.nextToken();
                    if (this.journalId.equals(jId)) {
                        if ("journal".equals(op)) {
                            journalRoot = newUuid;
                            if (journalPending != null && journalPending.equals(newUuid)) {
                                String pending = journalPending;
                                synchronized (pending) {
                                    journalPending = null;
                                    pending.notify();
                                }
                            }
                        } else if ("journalrej".equals(op)) {
                            if (journalPending != null && journalPending.equals(newUuid)) {
                                synchronized (journalPending) {
                                    journalPending.notify();
                                }
                            }
                        }
                    }
                }
            }, "ZeroMQ NodeStore Journal Event Handler");
            journalEventHandler.setDaemon(true);
            journalEventHandler.start();
        }
    }

    public String getUUThreadId() {
        return storeId + "-" + Thread.currentThread().getId();
    }

    @Override
    public void close() {
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

        // TODO: configure using OSGi config
        final ZeroMQNodeStoreBuilder builder = new ZeroMQNodeStoreBuilder();
        builder.initFromEnvironment();
        configure(builder.getJournalId(), builder.getClusterInstances(), builder.isWriteBackJournal(), builder.isWriteBackNodes(), builder.isRemoteReads(), builder.getInitJournal(), builder.getBackendPrefix(), builder.isLogNodeStates(), builder.getBlobCacheDir());

        init();
        whiteboard = new OsgiWhiteboard(ctx.getBundleContext());
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
        executor = new WhiteboardExecutor();
        executor.start(whiteboard);
        //registerCloseable(executor);
        observerTracker = new ObserverTracker(this);
        observerTracker.start(ctx.getBundleContext());
        //registerCloseable(observerTracker);
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
        setRoot(zmqNewSuperRoot.getUuid(), emptyNode.getUuid());
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
                nodeStateReader[0].get().send("journal " + journalId);
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
            return ZeroMQEmptyNodeState.UUID_NULL.toString();
        }

        String msg;
        while (true) {
            try {
                nodeStateReader[0].get().send("journal " + journalId + "-checkpoints");
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

    private boolean setRoot(String uuid, String olduuid) {
        if (writeBackJournal) {
            journalPending = uuid;
            synchronized (journalPending) {
                setRootRemote(null, uuid, olduuid);
                try {
                    journalPending.wait();
                    if (journalPending == null) {
                        return true;
                    } else {
                        journalPending = null;
                        return false;
                    }
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
        } else {
            journalRoot = uuid;
            return true;
        }
    }

    private synchronized void setCheckpointRoot(String uuid) {
        final String olduuid = checkpointRoot;
        checkpointRoot = uuid;
        if (writeBackJournal) {
            setRootRemote("checkpoints", uuid, olduuid); // TODO: do we need checkpoints in the repo?
        }
    }

    private void setRootRemote(String type, String uuid, String olduuid) {
        while (true) {
            synchronized (mergeRootMonitor) {
                try {
                    final ZMQ.Socket socket = nodeStateWriter[0].get();
                    socket.send(getUUThreadId() + " "
                                    + "journal" + " " + journalId + (type == null ? "" : "-" + type) + " "
                                    + uuid + " "
                                    + olduuid
                    );
                    socket.recvStr(); // ignore
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
    }

    private NodeState mergeRoot(String root, NodeState ns) {
        final ZeroMQNodeState superRoot = getSuperRoot();
        final NodeBuilder superRootBuilder = superRoot.builder();
        superRootBuilder.setChildNode(root, ns);
        ZeroMQNodeState newSuperRoot = (ZeroMQNodeState) superRootBuilder.getNodeState();
        while (!setRoot(newSuperRoot.getUuid(), superRoot.getUuid())) {
            rebase(superRootBuilder, getRoot());
            newSuperRoot = (ZeroMQNodeState) superRootBuilder.getNodeState();
            try {
                Thread.sleep(1000); // we don't want to spam the log
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
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
        countNodeRead();
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

    private void countNodeRead() {
        final long c = remoteReadNodeCounter.incrementAndGet();
        if (c % 1000 == 0) {
            log.info("Remote nodes read: {}", c);
        }
    }

    private void write(String event) {
        if (writeBackNodes) {
            try {
                final ZMQ.Socket writer = nodeStateWriter[0].get();
                writer.send(getUUThreadId() + " " + event);
                log.trace(writer.recvStr()); // ignore
            } catch (Throwable t) {
                log.error(t.getMessage());
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
            countNodeWritten();
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

    private void countNodeWritten() {
        final long c = remoteWriteNodeCounter.incrementAndGet();
        if (c % 1000 == 0) {
            log.info("Remote nodes written: {}", c);
        }
    }

    private void writeBlob(ZeroMQBlob blob) {
        if (!writeBackNodes) {
            return;
        }
        countBlobWritten();
        synchronized (writeMonitor) {
            try {
                LoggingHook.writeBlob(blob, this::write);
            } catch (Throwable t) {
                log.error(t.getMessage());
            }
        }
    }

    private void countBlobWritten() {
        final long c = remoteWriteBlobCounter.incrementAndGet();
        if (c % 1000 == 0) {
            log.info("Remote blobs written: {}", c);
        }
    }

    private InputStream readBlob(String reference) {
        if (!remoteReads) {
            return null;
        }
        countBlobRead();
        final InputStream ret = new ZeroMQBlobInputStream(nodeStateReader[0], "blob " + reference);
        return ret;
    }

    private void countBlobRead() {
        final long c = remoteReadBlobCounter.incrementAndGet();
        if (c % 1000 == 0) {
            log.info("Remote blobs read: {}", c);
        }
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

    /*
     * GarbageCollectableBlobStore
     */
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
}
