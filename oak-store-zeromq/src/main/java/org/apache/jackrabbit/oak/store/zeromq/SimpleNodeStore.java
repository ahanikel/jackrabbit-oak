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
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
@Service
public class SimpleNodeStore implements NodeStore, Observable, Closeable, GarbageCollectableBlobStore {

    private static final Logger log = LoggerFactory.getLogger(SimpleNodeStore.class.getName());
    public static final String ROOT_NODE_NAME = "root";

    public static SimpleNodeStoreBuilder builder() {
        return new SimpleNodeStoreBuilder();
    }

    public SimpleNodeState EMPTY;
    public SimpleNodeState MISSING;

    private ZContext context;
    private String journalId;
    private ZeroMQSocketProvider nodeStateReader;
    private ZeroMQSocketProvider nodeStateWriter;
    private KVStore<String, SimpleNodeState> nodeStateCache;
    private KVStore<String, SimpleBlob> blobCache;
    private volatile ChangeDispatcher changeDispatcher;

    private final Object mergeRootMonitor = new Object();
    private final Object checkpointMonitor = new Object();
    private final Object writeMonitor = new Object();

    private volatile String expectedRoot = null;
    private Thread logProcessor;
    private ZMQ.Socket journalSocket;
    private volatile String journalRoot;
    private volatile String checkpointRoot;
    private String initJournal;
    private LoggingHook loggingHook;
    private volatile boolean configured;
    private final String storeId = UUID.randomUUID().toString();
    private final AtomicLong remoteReadNodeCounter = new AtomicLong();
    private final AtomicLong remoteWriteNodeCounter = new AtomicLong();
    private final AtomicLong remoteReadBlobCounter = new AtomicLong();
    private final AtomicLong remoteWriteBlobCounter = new AtomicLong();
    private File blobCacheDir;
    private BlobStore store;

    public SimpleNodeStore() {
    }

    SimpleNodeStore(SimpleNodeStoreBuilder builder) {
        configure(
            builder.getJournalId(),
            builder.getInitJournal(),
            builder.getBackendReaderURL(),
            builder.getBackendWriterURL(),
            builder.getJournalSocketURL(),
            builder.getBlobCacheDir());
    }

    private void configure(
        String journalId,
        String initJournal,
        String backendReaderURL,
        String backendWriterURL,
        String journalSocketUrl,
        String blobCacheDir) {

        if (configured) {
            return;
        }

        this.configured = true;
        this.journalId = journalId;
        this.context = new ZContext();
        this.initJournal = initJournal;
        this.blobCacheDir = new File(blobCacheDir);
        this.blobCacheDir.mkdirs();
        try {
            this.store = new RemoteBlobStore(this::read, this::write, new SimpleBlobStore(this.blobCacheDir));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        this.EMPTY = SimpleNodeState.empty(store);
        this.MISSING = SimpleNodeState.missing(store);

        nodeStateReader = new ZeroMQSocketProvider(backendReaderURL, context, SocketType.REQ);
        nodeStateWriter = new ZeroMQSocketProvider(backendWriterURL, context, SocketType.REQ);

        final Cache<String, SimpleNodeState> cache =
                CacheBuilder.newBuilder()
                        .concurrencyLevel(10)
                        .maximumSize(200000).build();

        nodeStateCache = new NodeStateCache<>(cache, ref -> SimpleNodeState.get(store, ref));

        // TODO: this can probably be simplified similarly to the nodestates above.
        final Cache<String, SimpleBlob> bCache =
                CacheBuilder.newBuilder()
                        .concurrencyLevel(10)
                        .maximumSize(100).build();
        blobCache = new NodeStateCache<>(bCache, ref -> new SimpleBlob(store, ref));

        logProcessor = new Thread("ZeroMQ Log Processor") {
            public void run() {
                journalSocket = new ZeroMQSocketProvider(journalSocketUrl, context, SocketType.SUB).get();
                journalSocket.subscribe(journalId);
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
                                final SimpleNodeState newRoot = (SimpleNodeState) builder.getNodeState();
                                journalRoot = newRoot.getRef();
                            } catch (Throwable e) {
                                // ignore
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
                            break;
                        }
                    }
                }
            }
        };
        logProcessor.start();
        loggingHook = LoggingHook.newLoggingHook(this::write);
    }

    public String getUUThreadId() {
        return storeId + "-" + Thread.currentThread().getId();
    }

    @Override
    public void close() {
        nodeStateReader.close();
        nodeStateWriter.close();
        logProcessor.interrupt();
    }

    @Activate
    public void activate(ComponentContext ctx) {

        // TODO: configure using OSGi config
        final SimpleNodeStoreBuilder builder = new SimpleNodeStoreBuilder();
        builder.initFromEnvironment();
        configure(
            builder.getJournalId(),
            builder.getInitJournal(),
            builder.getBackendReaderURL(),
            builder.getBackendWriterURL(),
            builder.getJournalSocketURL(),
            builder.getBlobCacheDir());

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
        checkpointRoot = readCPRootRemote();
        log.info("Journal root initialised with {}", uuid);
        journalRoot = uuid;
        if ("undefined".equals(uuid) || SimpleNodeState.UUID_NULL.toString().equals(uuid)) {
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
        final NodeBuilder builder = EMPTY.builder();
        builder.setChildNode(ROOT_NODE_NAME);
        builder.setChildNode("checkpoints");
        final SimpleNodeState newSuperRoot = (SimpleNodeState) builder.getNodeState();
        journalRoot = EMPTY.getRef(); // the new journalRoot is set by the log processor
        setRoot(newSuperRoot.getRef(), EMPTY.getRef());
        setCheckpointRoot(EMPTY.getRef());
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
                nodeStateReader.get().send("journal " + journalId);
                nodeStateReader.get().recvStr().equals("E"); // verb, always "E"
                msg = nodeStateReader.get().recvStr();
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

    private String readCPRootRemote() {
        String msg;
        while (true) {
            try {
                nodeStateReader.get().send("journal " + journalId + "-checkpoints");
                nodeStateReader.get().recvStr().equals("E"); // verb, always "E"
                msg = nodeStateReader.get().recvStr();
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

    private NodeState getCheckpointRoot() {
        if ("undefined".equals(checkpointRoot) || checkpointRoot == null) {
            throw new IllegalStateException("checkpointRoot is undefined, forgot to call init()?");
        }
        return readNodeState(checkpointRoot);
    }

    private void setRoot(String uuid, String oldUuid) {
        setRootRemote(null, uuid, oldUuid);
        expectedRoot = uuid;
        synchronized (expectedRoot) {
            try {
                expectedRoot.wait();
                expectedRoot = null;
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private synchronized void setCheckpointRoot(String uuid) {
        final String oldUuid = checkpointRoot;
        checkpointRoot = uuid;
        setRootRemote("checkpoints", uuid, oldUuid);
    }

    private void setRootRemote(String type, String uuid, String oldUuid) {
        while (true) {
            synchronized (mergeRootMonitor) {
                try {
                    final ZMQ.Socket socket = nodeStateWriter.get();
                    socket.send(getUUThreadId() + " "
                                    + "journal" + " " + journalId + (type == null ? "" : "-" + type) + " "
                                    + uuid + " "
                                    + oldUuid
                    );
                    socket.recvStr(); // ignore
                    break;
                } catch (Throwable t) {
                    log.warn(t.toString());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }
        }
    }

    private void mergeRoot(NodeState ns) {
        final SimpleNodeState superRoot = getSuperRoot();
        final NodeBuilder superRootBuilder = superRoot.builder();
        superRootBuilder.setChildNode(ROOT_NODE_NAME, ns);
        final SimpleNodeState newSuperRoot = (SimpleNodeState) superRootBuilder.getNodeState();
        setRoot(newSuperRoot.getRef(), superRoot.getRef());
    }

    @Override
    @NotNull
    public NodeState merge(@NotNull NodeBuilder builder, @NotNull CommitHook commitHook, @NotNull CommitInfo info) throws CommitFailedException {
        synchronized (mergeRootMonitor) {
            if (!(builder instanceof SimpleNodeBuilder)) {
                throw new IllegalArgumentException();
            }
            final NodeState newBase = getRoot();
            // rebase does nothing if the base hasn't changed
            rebase(builder, newBase);
            final NodeState after = builder.getNodeState();
            final NodeState afterHook = commitHook.processCommit(newBase, after, info);
            if (afterHook.equals(newBase)) {
                return newBase;
            }
            mergeRoot(afterHook);
            ((SimpleNodeBuilder) builder).reset(afterHook);
            if (changeDispatcher != null) {
                changeDispatcher.contentChanged(afterHook, info);
            }
            return afterHook;
        }
    }

    private void mergeCheckpoint(NodeBuilder builder) {
        synchronized (checkpointMonitor) {
            final NodeState newBase = getCheckpointRoot();
            rebase(builder, newBase);
            final SimpleNodeState after = (SimpleNodeState) builder.getNodeState();
            setCheckpointRoot(after.getRef());
            ((SimpleNodeBuilder) builder).reset(after);
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
        final SimpleNodeState ret = nodeStateCache.get(ref);
        if (ret == null) {
            log.warn("Node not found: {} ", ref);
        }
        return ret;
    }

    private InputStream read(String uuid) {
        countNodeRead();
        return new ZeroMQBlobInputStream(nodeStateReader, uuid);
    }

    private void countNodeRead() {
        final long c = remoteReadNodeCounter.incrementAndGet();
        if (c % 1000 == 0) {
            log.info("Remote nodes read: {}", c);
        }
    }

    private synchronized void write(String event) {
        try {
            final ZMQ.Socket writer = nodeStateWriter.get();
            writer.send(getUUThreadId() + " " + event);
            final String msg = writer.recvStr();
            if (!msg.equals("E")) {
                log.error("{}: {}", msg, writer.recvStr());
            } else {
                writer.recvStr().equals("");
            }
        } catch (Throwable t) {
            log.error(t.getMessage());
        }
    }

    // TODO: will be used by SimpleNodeBuilder
    void write(SimpleNodeState before, SimpleNodeState after) {
        final String newUuid = after.getRef();
        if (nodeStateCache.isCached(newUuid)) {
            return;
        }
        nodeStateCache.put(newUuid, after);
        countNodeWritten();
        synchronized (writeMonitor) {
            loggingHook.processCommit(before, after, null);
        }
    }

    private void countNodeWritten() {
        final long c = remoteWriteNodeCounter.incrementAndGet();
        if (c % 1000 == 0) {
            log.info("Remote nodes written: {}", c);
        }
    }

    private void writeBlob(SimpleBlob blob) {
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
        reference = reference.toLowerCase(); // TODO: check
        countBlobRead();
        return new ZeroMQBlobInputStream(nodeStateReader, reference);
    }

    private boolean hasBlob(String reference) {
        reference = reference.toLowerCase(); // TODO: check
        ZMQ.Socket socket = nodeStateReader.get();
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
    @NotNull
    public NodeState rebase(@NotNull NodeBuilder builder) {
        final NodeState newBase = getRoot();
        return rebase(builder, newBase);
    }

    public NodeState rebase(@NotNull NodeBuilder builder, NodeState newBase) {
        checkArgument(builder instanceof SimpleNodeBuilder);
        NodeState head = checkNotNull(builder).getNodeState();
        NodeState base = builder.getBaseState();
        if (base != newBase) {
            ((SimpleNodeBuilder) builder).reset(newBase);
            head.compareAgainstBaseState(
                    base, new ConflictAnnotatingRebaseDiff(builder));
            head = builder.getNodeState();
        }
        return head;
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
        try {
            final String ref = store.putInputStream(inputStream);
            final SimpleBlob blob = new SimpleBlob(store, ref);
            writeBlob(blob);
            blobCache.put(blob.getReference(), blob);
            return blob;
        } catch (FileAlreadyExistsException e) {
            final String ref = e.getMessage();
            final SimpleBlob blob = new SimpleBlob(store, ref);
            blobCache.put(blob.getReference(), blob);
            return blob;
        }
    }

    @Override
    public Blob getBlob(@NotNull String reference) {
        if (reference == null) {
            return null;
        }
        reference = reference.toLowerCase(); // TODO: check
        final Blob ret = blobCache.get(reference);
        if (ret == null ||
            (!reference.equals("d41d8cd98f00b204e9800998ecf8427e") // TODO: check
                && ret.getReference().equals("d41d8cd98f00b204e9800998ecf8427e"))) {
            final String msg = "Blob " + reference + " not found";
            log.warn(msg);
            return null;
        }
        return ret;
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

        mergeCheckpoint(checkpoints);
        return name;
    }

    @Override
    @NotNull
    public synchronized String checkpoint(long lifetime) {
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
            mergeCheckpoint(cpRoot);
        }
        return ret;
    }

    @Override
    public Closeable addObserver(Observer observer) {
        return changeDispatcher.addObserver(observer);
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
    public String writeBlob(String tempFileName) throws IOException {
        return createBlob(new FileInputStream(tempFileName)).getReference();
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
}
