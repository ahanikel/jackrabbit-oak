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
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo.getOrCreateId;

@Component(
        scope = ServiceScope.SINGLETON,
        immediate = true,
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        property = "oak.nodestore.description=nodeStoreType=zeromq"
)
@Service
public class ZeroMQNodeStore implements NodeStore, Observable, Closeable, GarbageCollectableBlobStore {

    private static final Logger log = LoggerFactory.getLogger(ZeroMQNodeStore.class.getName());

    public static ZeroMQNodeStoreBuilder builder() {
        return new ZeroMQNodeStoreBuilder();
    }

    @NotNull
    ZContext context;
    private String journalId;

    private boolean writeBackJournal;
    private boolean writeBackNodes;
    private boolean remoteReads;

    @NotNull
    ZeroMQSocketProvider nodeStateReader;

    @NotNull
    ZeroMQSocketProvider nodeStateWriter;

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
    volatile String expectedRoot = null;

    private Thread logProcessor;
    private ZMQ.Socket journalSocket;

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

    private File blobCacheDir;

    public ZeroMQNodeStore() {
    }

    ZeroMQNodeStore(ZeroMQNodeStoreBuilder builder) {
        configure(builder.getJournalId(), builder.isWriteBackJournal(), builder.isWriteBackNodes(), builder.isRemoteReads(), builder.getInitJournal(), builder.getBackendReaderURL(), builder.getBackendWriterURL(), builder.getJournalSocketURL(), builder.isLogNodeStates(), builder.getBlobCacheDir());
    }

    private void configure(
        String journalId,
        boolean writeBackJournal,
        boolean writeBackNodes,
        boolean remoteReads,
        String initJournal,
        String backendReaderURL,
        String backendWriterURL,
        String journalSocketUrl,
        boolean logNodeStates,
        String blobCacheDir) {

        if (configured) {
            return;
        }

        configured = true;
        this.journalId = journalId;

        context = new ZContext();

        this.writeBackJournal = writeBackJournal;
        this.writeBackNodes = writeBackNodes;
        this.remoteReads = remoteReads;
        this.initJournal = initJournal;
        this.blobCacheDir = new File(blobCacheDir);
        this.blobCacheDir.mkdirs();

        nodeStateReader = new ZeroMQSocketProvider(backendReaderURL, context, SocketType.REQ);
        nodeStateWriter = new ZeroMQSocketProvider(backendWriterURL, context, SocketType.REQ);

        /*
            If we allow remote reads, we just cache the return values
            otherwise we store them in a map
         */
        if (remoteReads) {
            final Cache<String, ZeroMQNodeState> cache =
                    CacheBuilder.newBuilder()
                            .concurrencyLevel(10)
                            .maximumSize(200000).build();
            nodeStateCache = new NodeStateCache<>(cache, uuid -> {

                final String sNode = read(uuid);
                try {
                    final ZeroMQNodeState ret = ZeroMQNodeState.deserialise(this, sNode);
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
                            .maximumSize(100).build();
            blobCache = new NodeStateCache<>(bCache, reference -> {
                try {
                    ZeroMQBlob ret = ZeroMQBlob.newInstance(this.blobCacheDir, reference);
                    if (ret == null) {
                        ret = ZeroMQBlob.newInstance(this.blobCacheDir, reference, readBlob(reference));
                    }
                    return ret;
                } catch (Throwable t) {
                    log.warn("Could not load blob: " + t.toString());
                    return null;
                }
            });
            logProcessor = new Thread("ZeroMQ Log Processor") {
                public void run() {
                    journalSocket = context.createSocket(SocketType.SUB);
                    journalSocket.connect(journalSocketUrl);
                    journalSocket.subscribe(journalId);
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            final String journalId = journalSocket.recvStr();
                            // this test is necessary because the subscription only matches
                            // the beginning of the string
                            // e.g. golden-checkpoints matches, too.
                            if (!journalId.equals(ZeroMQNodeStore.this.journalId)) {
                                continue;
                            }
                            final String newUuid = journalSocket.recvStr();
                            final String oldUuid = journalSocket.recvStr();
                            log.info("Received {} {} ({})", journalId, newUuid, oldUuid);
                            if (oldUuid.equals(journalRoot)) {
                                journalRoot = newUuid;
                            } else {
                                try {
                                    final NodeState after = readNodeState(newUuid);
                                    final NodeState before = readNodeState(oldUuid);
                                    final NodeBuilder builder = readNodeState(journalRoot).builder();
                                    // TODO: when does a conflict lead to a CommitFailedException?
                                    final NodeStateDiff diff = new ConflictAnnotatingRebaseDiff(builder);
                                    after.compareAgainstBaseState(before, diff);
                                    final ZeroMQNodeState newRoot = (ZeroMQNodeState) builder.getNodeState();
                                    journalRoot = newRoot.getUuid();
                                } catch (Throwable e) {
                                }
                            }
                            if (expectedRoot != null && expectedRoot.equals(newUuid)) {
                                log.info("Found our commit {}", newUuid);
                                synchronized (expectedRoot) {
                                    expectedRoot.notify();
                                }
                            }
                        } catch (Throwable t) {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                            }
                        }
                    }
                }
            };
            logProcessor.start();
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
    }

    public String getUUThreadId() {
        return storeId + "-" + Thread.currentThread().getId();
    }

    @Override
    public void close() {
        nodeStateReader.close();
        nodeStateWriter.close();
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
        configure(builder.getJournalId(), builder.isWriteBackJournal(), builder.isWriteBackNodes(), builder.isRemoteReads(), builder.getInitJournal(), builder.getBackendReaderURL(), builder.getBackendWriterURL(), builder.getJournalSocketURL(), builder.isLogNodeStates(), builder.getBlobCacheDir());

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
        if (ZeroMQEmptyNodeState.UUID_NULL.toString().equals(uuid)) {
            reset();
        }
        changeDispatcher = new ChangeDispatcher(getRoot());
    }

    void reInit() {
        emptyCaches();
        init();
    }

    /**
     * Wipe the complete repo by setting the root to an empty node. The existing
     * nodes remain lingering around unless some GC mechanism (which is not yet
     * implemented) removes them. This is needed for testing.
     */
    public void reset() {
        emptyCaches();
        final NodeBuilder builder = emptyNode.builder();
        builder.setChildNode("root");
        builder.setChildNode("checkpoints");
        builder.setChildNode("blobs");
        final NodeState newSuperRoot = builder.getNodeState();
        final ZeroMQNodeState zmqNewSuperRoot = (ZeroMQNodeState) newSuperRoot;
        journalRoot = emptyNode.getUuid();
        setRoot(zmqNewSuperRoot.getUuid(), emptyNode.getUuid());
        setCheckpointRoot(emptyNode.getUuid());
    }

    void emptyCaches() {
        nodeStateCache.empty();
        blobCache.empty();
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
                nodeStateReader.get().send("journal " + journalId);
                nodeStateReader.get().recv(); // verb, always "E"
                msg = nodeStateReader.get().recvStr();
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
                nodeStateReader.get().send("journal " + journalId + "-checkpoints");
                nodeStateReader.get().recvStr(); // verb, always "E"
                msg = nodeStateReader.get().recvStr();
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

    private void setRoot(String uuid, String olduuid) {
        if (writeBackJournal) {
            if (expectedRoot != null) {
                log.error("Expected root is not null but {}, continuing.", expectedRoot);
            }
            expectedRoot = uuid;
            synchronized (expectedRoot) {
                try {
                    setRootRemote(null, uuid, olduuid);
                    expectedRoot.wait();
                    expectedRoot = null;
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    private synchronized void setCheckpointRoot(String uuid) {
        final String olduuid = checkpointRoot;
        if (writeBackJournal) {
            checkpointRoot = uuid;
            setRootRemote("checkpoints", uuid, olduuid);
        }
    }

    private void setRootRemote(String type, String uuid, String olduuid) {
        while (true) {
            synchronized (mergeRootMonitor) {
                try {
                    final ZeroMQSocketProvider.Socket socket = nodeStateWriter.get();
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
        final ZeroMQNodeState newSuperRoot = (ZeroMQNodeState) superRootBuilder.getNodeState();
        setRoot(newSuperRoot.getUuid(), superRoot.getUuid());
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
        final ZeroMQSocketProvider.Socket socket = nodeStateReader.get();
        while (true) {
            msg = new StringBuilder();
            try {
                socket.send(uuid);
                do {
                    socket.recvStr(); // verb, always "E"
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
                final ZeroMQSocketProvider.Socket writer = nodeStateWriter.get();
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
        reference = reference.toLowerCase();
        countBlobRead();
        final InputStream ret = new ZeroMQBlobInputStream(nodeStateReader, "blob " + reference);
        return ret;
    }

    private boolean hasBlob(String reference) {
        reference = reference.toLowerCase();
        ZeroMQSocketProvider.Socket socket = nodeStateReader.get();
        socket.send("hasblob " + reference);
        socket.recvStr(); // always "E"
        final String ret = socket.recvStr();
        if ("true".equals(ret)) {
            return true;
        } else if ("false".equals(ret)) {
            return false;
        } else {
            throw new IllegalStateException("hasBlob returned " + ret);
        }
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
        final ZeroMQBlob blob = ZeroMQBlob.newInstance(blobCacheDir, inputStream);
        if (!hasBlob(blob.getReference())) {
            writeBlob(blob);
        }
        if (blobCache.get(blob.getReference()) == null) {
            blobCache.put(blob.getReference(), blob);
        }
        return blob;
    }

    ZeroMQBlob createBlob(Blob blob) throws IOException {
        ZeroMQBlob ret;
        if (blob instanceof ZeroMQBlob) {
            ret = (ZeroMQBlob) blob;
            if (blobCache.get(ret.getReference()) == null) {
                writeBlob(ret); // should not happen
            }
            blobCache.put(ret.getReference(), ret);
            return ret;
        }
        ret = ZeroMQBlob.newInstance(blobCacheDir, blob.getNewStream());
        if (getBlob(ret.getReference()) == null) {
            writeBlob(ret);
            blobCache.put(ret.getReference(), ret);
        }
        return ret;
    }

    @Override
    public Blob getBlob(String reference) {
        if (reference == null) {
            return null;
        }
        Blob ret = null;
        while (ret == null) {
            try {
                reference = reference.toLowerCase();
                ret = blobCache.get(reference);
                if (ret == null ||
                    (!reference.equals("d41d8cd98f00b204e9800998ecf8427e")
                        && ret.getReference().equals("d41d8cd98f00b204e9800998ecf8427e"))) {
                    final String msg = "Blob " + reference + " not found";
                    log.warn(msg);
                    return null;
                }
            } catch (Exception e) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException interruptedException) {
                }
            }
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

        public void empty();
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

        @Override
        public void empty() {
            store.clear();
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

        @Override
        public void empty() {
            cache.invalidateAll();
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
