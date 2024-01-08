package org.apache.jackrabbit.oak.store.migration;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class MigrationNodeStore implements NodeStore {

    private final NodeStore nsSource;
    private final NodeStore nsTo;
    private final Thread nodeStoreCopier;

    private enum Status {
        START,
        SWITCHING,
        END
    }

    private volatile Status status;

    private NodeStore getBackend() {
        switch (status) {
            case START:
                return nsSource;
            case SWITCHING:
                while (status == Status.SWITCHING) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                return getBackend();
            case END:
                return nsTo;
        }
        throw new IllegalStateException("Unknown and unexpected status " + status);
    }

    private class LoggingApplyDiff extends ApplyDiff {
        // TODO: when we come back from nodeAdded or nodeChanged, log completion of the path
        //       if path.length < a certain size
        public LoggingApplyDiff(NodeBuilder builder) {
            super(builder);
        }
    }

    private class NodeStoreCopier implements Runnable {
        @Override
        public void run() {
            NodeBuilder builder = nsTo.getRoot().builder();
            ((MemoryNodeBuilder) builder).getPath();
            nsSource.getRoot().compareAgainstBaseState(EmptyNodeState.EMPTY_NODE, new LoggingApplyDiff(builder));
        }
        // FIXME: how can we interrupt copying stuff and resume?
        //        possible solution:
        //        a combination of root nodestate plus path could represent the current state
        //        the root nodestate can be serialised as a checkpoint
        //        this could also solve the "checkpoint problem"
    }

    public MigrationNodeStore(NodeStore nsSource, NodeStore nsTo) {
        this.nsSource = nsSource;
        this.nsTo = nsTo;
        this.status = Status.START;
        this.nodeStoreCopier = new Thread(new NodeStoreCopier(), "Oak Migration NodeStoreCopier");
        this.nodeStoreCopier.start();
    }

    @Override
    public @NotNull NodeState getRoot() {
        return getBackend().getRoot();
    }

    @Override
    public @NotNull NodeState merge(@NotNull NodeBuilder nodeBuilder, @NotNull CommitHook commitHook, @NotNull CommitInfo commitInfo) throws CommitFailedException {
        return getBackend().merge(nodeBuilder, commitHook, commitInfo); // FIXME: check nodeBuilder
    }

    @Override
    public @NotNull NodeState rebase(@NotNull NodeBuilder nodeBuilder) {
        return getBackend().rebase(nodeBuilder); // FIXME: check nodeBuilder
    }

    @Override
    public NodeState reset(@NotNull NodeBuilder nodeBuilder) {
        return getBackend().reset(nodeBuilder); // FIXME: check nodeBuilder
    }

    @Override
    public @NotNull Blob createBlob(InputStream inputStream) throws IOException {
        return getBackend().createBlob(inputStream);
    }

    @Override
    public @Nullable Blob getBlob(@NotNull String s) {
        return getBackend().getBlob(s);
    }

    @Override
    public @NotNull String checkpoint(long l, @NotNull Map<String, String> map) {
        return getBackend().checkpoint(l, map);
    }

    @Override
    public @NotNull String checkpoint(long l) {
        return getBackend().checkpoint(l);
    }

    @Override
    public @NotNull Map<String, String> checkpointInfo(@NotNull String s) {
        return getBackend().checkpointInfo(s);
    }

    @Override
    public @NotNull Iterable<String> checkpoints() {
        return getBackend().checkpoints();
    }

    @Override
    public @Nullable NodeState retrieve(@NotNull String s) {
        return getBackend().retrieve(s);
    }

    @Override
    public boolean release(@NotNull String s) {
        return getBackend().release(s);
    }
}
