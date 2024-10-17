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

import com.google.common.base.Charsets;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.jackrabbit.commons.SimpleValueFactory;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHook;
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
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
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
import org.zeromq.ZMQException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
        property = "oak.nodestore.description=nodeStoreType=simple"
)
public class SimpleNodeStore implements NodeStore, Observable, Closeable, GarbageCollectableBlobStore, Clusterable {

    private static final Logger log = LoggerFactory.getLogger(SimpleNodeStore.class.getName());
    public static final String ROOT_NODE_NAME = "root";
    public static final String CHECKPOINT_NODE_NAME = "checkpoints";
    public static SimpleNodeStoreBuilder builder() {
        return new SimpleNodeStoreBuilder();
    }

    public SimpleNodeState EMPTY;
    public SimpleNodeState MISSING;

    private ZContext context;
    private String journalId;
    private SimpleRequestResponse nodeStateReader;
    private SimpleRequestResponse nodeStateWriter;
    private KVStore<String, SimpleNodeState> nodeStateCache;
    private KVStore<String, SimpleBlob> blobCache;
    private volatile ChangeDispatcher changeDispatcher;

    private final Map<String, Pair<Object, CommitInfo>> expectedRoots = new ConcurrentHashMap<>();
    private final Map<String, Object> confirmedRoots = new ConcurrentHashMap<>();
    private volatile boolean commitFailed = false;
    private Thread logProcessor;
    private ZMQ.Socket journalSocket;
    private volatile String journalRoot;
    private volatile String checkpointRoot;
    private String initJournal;
    private volatile boolean configured;
    private final AtomicLong remoteReadNodeCounter = new AtomicLong();
    private final AtomicLong remoteWriteBlobCounter = new AtomicLong();
    private File blobCacheDir;
    private BlobStore blobStore;
    private String instanceId;
    private BloomFilter<CharSequence> roots = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), 9000000); // about 8MB

    public SimpleNodeStore() {
    }

    SimpleNodeStore(SimpleNodeStoreBuilder builder) {
        configure(
            builder.getJournalId(),
            builder.getInitJournal(),
            builder.getBackendReaderURL(),
            builder.getBackendWriterURL(),
            builder.getBlobCacheDir(),
            builder.getAzureStorageConnectionString(),
            builder.getAzureContainerName(),
            builder.getS3Endpoint(),
            builder.getS3SigningRegion(),
            builder.getS3AccessKey(),
            builder.getS3SecretKey(),
            builder.getS3ContainerName(),
            builder.getS3FolderName());
    }

    private void configure(
        String journalId,
        String initJournal,
        String backendReaderURL,
        String backendWriterURL,
        String blobCacheDir,
        String azureStorageConnectionString,
        String azureContainerName,
        String s3Endpoint,
        String s3SigningRegion,
        String s3AccessKey,
        String s3SecretKey,
        String s3ContainerName,
        String s3FolderName
        ) {

        if (configured) {
            return;
        }

        this.configured = true;
        this.journalId = journalId;
        this.context = new ZContext();
        this.initJournal = initJournal;
        this.blobCacheDir = new File(blobCacheDir);
        this.blobCacheDir.mkdirs();

        this.EMPTY = SimpleNodeState.empty(this);
        this.MISSING = SimpleNodeState.missing(this);

        nodeStateReader = new SimpleRequestResponse(SimpleRequestResponse.Topic.READ, backendWriterURL, backendReaderURL);
        nodeStateWriter = new SimpleRequestResponse(SimpleRequestResponse.Topic.WRITE, backendWriterURL, backendReaderURL);

        /*
        if (azureStorageConnectionString != null && !azureStorageConnectionString.isEmpty()) {
            blobStoreAdapter = new AzureBlobStoreAdapter(azureStorageConnectionString, azureContainerName);
        } else
        */
        RemoteBlobStore remoteBlobStore;
        if (s3Endpoint != null && !s3Endpoint.isEmpty()) {
            remoteBlobStore = new S3BlobStore(s3Endpoint, s3SigningRegion, s3AccessKey, s3SecretKey, s3ContainerName, s3FolderName);
        } else {
            remoteBlobStore = new ZeroMQBlobStore(nodeStateReader, nodeStateWriter);
        }
        try {
            this.blobStore = new SimpleRemoteBlobStore(new SimpleBlobStore(this.blobCacheDir), remoteBlobStore);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        final Cache<String, SimpleNodeState> cache =
                CacheBuilder.newBuilder()
                        .concurrencyLevel(10)
                        .maximumSize(200000).build();

        nodeStateCache = new NodeStateCache<>(cache, ref -> SimpleNodeState.get(this, ref));

        final Cache<String, SimpleBlob> bCache =
                CacheBuilder.newBuilder()
                        .concurrencyLevel(10)
                        .maximumSize(100000).build();
        blobCache = new NodeStateCache<>(bCache, ref -> SimpleBlob.get(this, ref));

        logProcessor = new Thread("ZeroMQ Log Processor") {
            public void run() {
                journalSocket = new ZeroMQSocketProvider(backendReaderURL, context, SocketType.SUB).get();
                journalSocket.subscribe(SimpleRequestResponse.Topic.JOURNAL.toString());
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        final String journalId = journalSocket.recvStr();
                        // this test is necessary because the subscription only matches
                        // the beginning of the string
                        // e.g. golden-checkpoints matches, too.
                        if (!journalId.equals(SimpleNodeStore.this.journalId)) {
                            continue;
                        }
                        final String newUuid = journalSocket.recvStr();
                        final String oldUuid = journalSocket.recvStr();
                        log.info("Received {} {} ({})", journalId, newUuid, oldUuid);
                        final NodeState newHead = readNodeState(newUuid);
                        if (oldUuid.equals(journalRoot)) {
                            journalRoot = newUuid;
                            roots.put(newUuid);
                            log.info("new root: {}", journalRoot);
                        } else if (newUuid.equals(journalRoot)) {
                            log.info("Already applied");
                            Pair<Object, CommitInfo> monitorInfo = expectedRoots.remove(newUuid);
                            if (monitorInfo != null) {
                                confirmedRoots.put(newUuid, monitorInfo);
                                synchronized (monitorInfo.fst) {
                                    monitorInfo.fst.notify();
                                }
                            }
                            continue;
                        } else {
                            NodeState oldBase = readNodeState(oldUuid);
                            NodeState newBase = readNodeState(journalRoot);
                            NodeBuilder newBuilder = newBase.builder();
                            NodeState newRoot = null;
                            try {
                                newRoot = rebase(newHead, oldBase, newBase, newBuilder);
                            } catch (CommitFailedException cfe) {
                                log.warn("Conflicting updates, commit failed: journal {} new: {} old: {} journalRoot: {}", journalId, newUuid, oldUuid, journalRoot);
                                commitFailed = true;
                            }
                            if (!commitFailed) {
                                journalRoot = ((SimpleNodeState) newRoot).getRef();
                                roots.put(newUuid);
                                roots.put(journalRoot);
                                log.info("new root after resolution: {}", journalRoot);
                                Thread.sleep(1000); // for composum
                            }
                        }
                        Pair<Object, CommitInfo> monitorInfo = expectedRoots.remove(newUuid);
                        if (monitorInfo != null) {
                            log.info("Found our commit {}", newUuid);
                            confirmedRoots.put(newUuid, monitorInfo);
                            if (!commitFailed) {
                                if (changeDispatcher != null) {
                                    changeDispatcher.contentChanged(newHead.getChildNode(ROOT_NODE_NAME), monitorInfo.snd);
                                }
                            }
                            synchronized (monitorInfo.fst) {
                                monitorInfo.fst.notify();
                            }
                        } else {
                            if (commitFailed) {
                                commitFailed = false;
                            } else {
                                if (changeDispatcher != null) {
                                    changeDispatcher.contentChanged(newHead.getChildNode(ROOT_NODE_NAME), CommitInfo.EMPTY_EXTERNAL);
                                }
                            }
                        }
                    } catch (Exception t) {
                        if (t instanceof ZMQException && t.getMessage().equals("Errno 4")) {
                            break;
                        }
                        log.warn("Caught exception: {}", t);
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                }
            }
        };
        logProcessor.start();
    }

    @Override
    public void close() {
        nodeStateReader.close();
        nodeStateWriter.close();
        logProcessor.interrupt();
    }

    @Activate
    public void activate(ComponentContext ctx) {

        instanceId = UUID.randomUUID().toString();

        // TODO: configure using OSGi config
        final SimpleNodeStoreBuilder builder = new SimpleNodeStoreBuilder();
        builder.initFromEnvironment();
        configure(
            builder.getJournalId(),
            builder.getInitJournal(),
            builder.getBackendReaderURL(),
            builder.getBackendWriterURL(),
            builder.getBlobCacheDir(),
            builder.getAzureStorageConnectionString(),
            builder.getAzureContainerName(),
            builder.getS3Endpoint(),
            builder.getS3SigningRegion(),
            builder.getS3AccessKey(),
            builder.getS3SecretKey(),
            builder.getS3ContainerName(),
            builder.getS3FolderName());

        init();
        OsgiWhiteboard whiteboard = new OsgiWhiteboard(ctx.getBundleContext());
        org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean
                (whiteboard
                        , CheckpointMBean.class
                        , new SimpleCheckpointMBean(this)
                        , CheckpointMBean.TYPE
                        , "SimpleNodeStore checkpoint management"
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
        whiteboard.register(Descriptors.class, new SimpleDiscoveryLiteDescriptors(this), new HashMap<>());
        WhiteboardExecutor executor = new WhiteboardExecutor();
        executor.start(whiteboard);
        //registerCloseable(executor);
        ObserverTracker observerTracker = new ObserverTracker(this);
        observerTracker.start(ctx.getBundleContext());
        //registerCloseable(observerTracker);
    }

    void init() {
        final String uuid = readRootRemote();
        log.info("Journal root initialised with {}", uuid);
        journalRoot = uuid;
        if ("undefined".equals(uuid) || SimpleNodeState.UUID_NULL.toString().equals(uuid)) {
            resetRoot();
        }
        resetCPRoot();
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
    public void reset () {
        resetRoot();
        resetCPRoot();
    }

    private void resetRoot() {
        emptyCaches();
        final NodeBuilder builder = EMPTY.builder();
        builder.setChildNode(ROOT_NODE_NAME);
        builder.setChildNode(CHECKPOINT_NODE_NAME);
        final SimpleNodeState newSuperRoot = (SimpleNodeState) builder.getNodeState();
        journalRoot = EMPTY.getRef(); // the new journalRoot is set by the log processor
        try {
            setRoot(newSuperRoot.getRef(), EMPTY.getRef(), CommitInfo.EMPTY);
        } catch (CommitFailedException e) {
            // should not happen
            throw new IllegalStateException(e);
        }
    }

    private void resetCPRoot() {
        SimpleNodeBuilder cpRootBuilder = (SimpleNodeBuilder) EMPTY.builder();
        cpRootBuilder.setChildNode(CHECKPOINT_NODE_NAME);
        checkpointRoot = cpRootBuilder.getNodeState().getRef();
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
        String msg;
        while (true) {
            try {
                nodeStateReader.requestString("journal", journalId).equals("E"); // verb, always "E"
                msg = nodeStateReader.receiveMore();
                break;
            } catch (Exception e) {
                log.warn(e.toString());
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    // ignore
                }
            }
        }
        return msg;
    }

    /*
    private String readCPRootRemote() {
        String msg;
        while (true) {
            try {
                nodeStateReader.requestString("journal", journalId + "-checkpoints").equals("E"); // verb, always "E"
                msg = nodeStateReader.receiveMore();
                break;
            } catch (Throwable t) {
                log.warn(t.toString());
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    // ignore
                }
            }
        }
        return msg;
    }
    */

    @Override
    @NotNull
    public NodeState getRoot() {
        return getSuperRoot().getChildNode(ROOT_NODE_NAME);
    }

    public SimpleNodeState getSuperRoot() {
        final String uuid = readRoot();
        if ("undefined".equals(uuid)) {
            throw new IllegalStateException("root is undefined, forgot to call init()?");
        }
        return readNodeState(uuid);
    }

    private SimpleNodeState getCheckpointSuperRoot() {
        if ("undefined".equals(checkpointRoot) || checkpointRoot == null) {
            throw new IllegalStateException("checkpointRoot is undefined, forgot to call init()?");
        }
        return readNodeState(checkpointRoot);
    }

    private SimpleNodeState getCheckpointRoot() {
        return (SimpleNodeState) getCheckpointSuperRoot().getChildNode(CHECKPOINT_NODE_NAME);
    }

    private void setRoot(String uuid, String oldUuid, CommitInfo info) throws CommitFailedException {
        final Object monitor = new Object();
        expectedRoots.put(uuid, Pair.of(monitor, info));
        synchronized (monitor) {
            try {
                for (int i = 0; ; ++i) {
                    setRootRemote(null, uuid, oldUuid);
                    monitor.wait(10000);
                    if (confirmedRoots.containsKey(uuid)) {
                        if (confirmedRoots.remove(uuid) != null) {
                            if (commitFailed) {
                                commitFailed = false;
                                throw new CommitFailedException("Conflict", 0, "");
                            }
                            break;
                        }
                    } else if (i > 8) {
                        throw new CommitFailedException("Error", 0, "Unsuccessful after " + i + " retries");
                    } else {
                        // timeout => try again
                    }
                }
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /*
    private void setCheckpointRoot(String uuid) {
        final String oldUuid = checkpointRoot;
        checkpointRoot = uuid;
        setRootRemote(CHECKPOINT_NODE_NAME, uuid, oldUuid);
    }
    */

    private void setRootRemote(String type, String uuid, String oldUuid) {
        while (true) {
            try {
                String msg = nodeStateWriter.requestString("journal",
                        journalId + (type == null ? "" : "-" + type) + " "
                                + uuid + " "
                                + oldUuid
                );
                if (!msg.equals("E")) {
                    log.error("lastReq: {}", nodeStateWriter.getLastReq());
                    log.error("{}: {}", msg, nodeStateWriter.receiveMore());
                } else {
                    nodeStateWriter.receiveMore(); // ignore, should be ""
                }
                break;
            } catch (Exception e1) {
                log.warn(e1.toString());
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
    }

    private void mergeRoot(NodeState ns, CommitInfo info) throws CommitFailedException {
        final SimpleNodeState superRoot = getSuperRoot();
        final NodeBuilder superRootBuilder = superRoot.builder();
        superRootBuilder.setChildNode(ROOT_NODE_NAME, ns);
        final SimpleNodeState newSuperRoot = (SimpleNodeState) superRootBuilder.getNodeState();
        setRoot(newSuperRoot.getRef(), superRoot.getRef(), info);
    }

    @Override
    @NotNull
    public NodeState merge(@NotNull NodeBuilder builder, @NotNull CommitHook commitHook, @NotNull CommitInfo info) throws CommitFailedException {
        if (!(builder instanceof SimpleNodeBuilder)) {
            throw new IllegalArgumentException();
        }
        checkArgument(((SimpleNodeBuilder) builder).isRoot());
        final NodeState before = builder.getBaseState();

        for (int retried = 0;; ++retried) {
            if (retried > 0) {
                log.info("Retrying merge: #{}", retried);
                try {
                    Thread.sleep(5000 + new Random().nextInt(5000));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            final NodeState newBase = getRoot();
            final NodeState afterConflict;
            if (!before.equals(newBase)) {
                afterConflict = rebase(builder, newBase);
            } else {
                afterConflict = builder.getNodeState();
            }
            try {
                final NodeState afterHook = commitHook.processCommit(newBase, afterConflict, info);
                if (afterHook.equals(newBase)) {
                    return newBase;
                }
                mergeRoot(afterHook, info);
                NodeState committed = getRoot();
                if (retried > 0) {
                    log.info("Commit successful after retrying {} times.", retried);
                }
                ((SimpleNodeBuilder) builder).reset(committed);
                return committed;
            } catch (CommitFailedException e) {
                if (retried > 90) {
                    log.error("Commit unsuccessful after retrying {} times. Giving up", retried);
                    throw e;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                }
            }
        }
    }

    private synchronized void mergeCheckpointRoot(NodeState cpRoot, CommitInfo info) throws CommitFailedException {
        final SimpleNodeState superRoot = getCheckpointSuperRoot();
        final NodeBuilder superRootBuilder = superRoot.builder();
        superRootBuilder.setChildNode(CHECKPOINT_NODE_NAME, cpRoot);
        final SimpleNodeState newSuperRoot = (SimpleNodeState) superRootBuilder.getNodeState();
        checkpointRoot = newSuperRoot.getRef();
    }

    private synchronized void mergeCheckpoint(NodeBuilder builder) throws CommitFailedException {
        int retried = 0;
        final NodeState before = builder.getBaseState();
        final NodeState after = builder.getNodeState();
        while (true) {
            final NodeState newBase = getCheckpointRoot();
            final NodeState afterConflict;
            if (!before.equals(newBase)) {
                final NodeBuilder newBuilder = newBase.builder();
                after.compareAgainstBaseState(before, new ApplyDiff(newBuilder));
                afterConflict = newBuilder.getNodeState();
            } else {
                afterConflict = after;
            }
            try {
                if (afterConflict.equals(newBase)) {
                    return;
                }
                mergeCheckpointRoot(afterConflict, CommitInfo.EMPTY);
                NodeState committed = getCheckpointRoot();
                if (retried > 0) {
                    log.info("Commit successful after retrying {} times.", retried);
                }
                ((SimpleNodeBuilder) builder).reset(committed);
                return;
            } catch (CommitFailedException e) {
                if (++retried > 0) {
                    log.error("Commit unsuccessful after trying {} times. Giving up", retried);
                    throw e;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                }
                continue;
            }
        }
    }

    @Nullable
    public SimpleNodeState readNodeState(String ref) {
        if (log.isTraceEnabled()) {
            log.trace("{} n? {}", Thread.currentThread().getId(), ref);
        }
        if (EMPTY.getRef().equals(ref)) {
            return EMPTY;
        }
        try {
            return nodeStateCache.get(ref);
        } catch (Exception e) {
            log.warn("Node not found: {} ", ref);
        }
        return null;
    }

    private void countNodeRead() {
        final long c = remoteReadNodeCounter.incrementAndGet();
        if (c % 1000 == 0) {
            log.info("Remote nodes read: {}", c);
        }
    }

    private void countBlobWritten() {
        final long c = remoteWriteBlobCounter.incrementAndGet();
        if (c % 1000 == 0) {
            log.info("Remote blobs written: {}", c);
        }
    }

    @Override
    @NotNull
    public NodeState rebase(@NotNull NodeBuilder builder) {
        final NodeState newBase = getRoot();
        try {
            return rebase(builder, newBase);
        } catch (CommitFailedException e) {
            throw new RuntimeException(e);
        }
    }

    public NodeState rebase(@NotNull NodeBuilder builder, NodeState newBase) throws CommitFailedException {
        checkArgument(builder instanceof SimpleNodeBuilder);
        checkArgument(newBase instanceof SimpleNodeState);
        SimpleNodeState head = (SimpleNodeState) checkNotNull(builder).getNodeState();
        SimpleNodeState base = (SimpleNodeState) builder.getBaseState();
        if (!base.equals(newBase)) {
            try {
                ((SimpleNodeBuilder) builder).reset(newBase);
            } catch (IllegalStateException e) {
                throw new IllegalArgumentException(e);
            }
            return rebase(head, base, newBase, builder);
        }
        return head;
    }

    public static NodeState rebase(NodeState newHead, NodeState oldBase, NodeState newBase, NodeBuilder newBuilder)
            throws CommitFailedException {
        newHead.compareAgainstBaseState(oldBase, new ConflictAnnotatingRebaseDiff(newBuilder));
        ConflictHook conflictHook = new ConflictHook(new SimpleConflictHandler());
        newHead = newBuilder.getNodeState();
        return conflictHook.processCommit(newBase, newHead, CommitInfo.EMPTY);
    }

    @Override
    public NodeState reset(@NotNull NodeBuilder builder) {
        final NodeState newBase = getRoot();
        ((MemoryNodeBuilder) builder).reset(newBase);
        return newBase;
    }

    @Override
    @NotNull
    public Blob createBlob(InputStream inputStream) throws IOException {
        String ref;
        try {
            ref = blobStore.putInputStream(inputStream);
            countBlobWritten();
        } catch (BlobAlreadyExistsException e) {
            ref = e.getRef();
        }
        final SimpleBlob blob = SimpleBlob.get(this, ref);
        blobCache.put(blob.getReference(), blob);
        return blob;
    }

    @Override
    @Nullable
    public Blob getBlob(@NotNull String reference) {
        if (reference == null) {
            return null;
        }
        assert(reference.equals(reference.toUpperCase()));
        try {
            return blobCache.get(reference);
        } catch (Exception e) {
            final String msg = "Blob " + reference + " not found";
            log.warn(msg);
        }
        return null;
    }

    @Override
    public synchronized @NotNull String checkpoint(long lifetime, @NotNull Map<String, String> properties) {
        long now = System.currentTimeMillis(); // is lifetime millis or micros?

        NodeBuilder checkpoints = getCheckpointRoot().builder();

        for (String n : checkpoints.getChildNodeNames()) {
            NodeBuilder cp = checkpoints.getChildNode(n);
            PropertyState ts = cp.getProperty("timestamp");
            if (ts == null || ts.getType() != LONG || now > ts.getValue(LONG)) {
                cp.remove();
            }
        }

        final SimpleNodeState currentRoot = (SimpleNodeState) getRoot();
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

        try {
            mergeCheckpoint(checkpoints);
        } catch (CommitFailedException e) {
            throw new IllegalStateException(e);
        }
        return name;
    }

    @Override
    @NotNull
    public String checkpoint(long lifetime) {
        return checkpoint(lifetime, Collections.emptyMap());
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
    @NotNull
    public Iterable<String> checkpoints() {
        final NodeState cpRoot = getCheckpointRoot();
        return cpRoot.getChildNodeNames();
    }

    @Override
    @Nullable
    public NodeState retrieve(@NotNull String checkpoint) {
        checkNotNull(checkpoint);
        final NodeState cpRoot = getCheckpointRoot();
        final NodeState cp = cpRoot.getChildNode(checkpoint).getChildNode(ROOT_NODE_NAME);
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
            try {
                mergeCheckpoint(cpRoot);
            } catch (CommitFailedException e) {
                throw new IllegalStateException(e);
            }
        }
        return ret;
    }

    @Override
    public Closeable addObserver(Observer observer) {
        return changeDispatcher.addObserver(observer);
    }

    @Override
    public @NotNull String getInstanceId() {
        return instanceId;
    }

    @Override
    public @Nullable String getVisibilityToken() {
        return journalRoot;
    }

    @Override
    public boolean isVisible(@NotNull String visibilityToken, long maxWaitMillis) throws InterruptedException {
        long maxWaitNanos = System.nanoTime() + maxWaitMillis * 1000000;
        do {
            // TODO: this is a very approximate implementation, is it good enough?
            // TODO: also, there is a race condition here
            if (journalRoot.equals(visibilityToken) || expectedRoots.containsKey(visibilityToken) || roots.mightContain(visibilityToken)) {
                return true;
            }
        } while (System.nanoTime() < maxWaitNanos);
        return false;
    }

    private interface KVStore<K, V> {
        @Nullable
        V get(K key);

        void put(K key, V value);

        boolean isCached(K key);

        void empty();
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
    public int sweep() throws IOException {
        return 0;
    }

    @Override
    public void startMark() {
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
    public Iterator<String> getAllChunkIds(long maxLastModifiedTime) {
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
    @Deprecated
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
    public String writeBlob(String tempFileName) throws IOException {
        try {
            return getBlobStore().putTempFile(new File(tempFileName));
        } catch (BlobAlreadyExistsException e) {
            return e.getRef();
        }
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
    public long getBlobLength(String blobId) {
        return getBlob(blobId).length();
    }

    @Override
    public InputStream getInputStream(String blobId) {
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

    public BlobStore getBlobStore() {
        return blobStore;
    }
}
