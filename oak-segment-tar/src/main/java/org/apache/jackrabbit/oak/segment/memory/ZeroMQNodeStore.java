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

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.UUID;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.LoggingHook;
import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import static org.apache.jackrabbit.oak.segment.memory.ZeroMQNodeState.newZeroMQNodeState;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 * A store which dumps everything into a queue.
 */
public class ZeroMQNodeStore implements NodeStore {

    private static final Logger log = LoggerFactory.getLogger(ZeroMQNodeStore.class.getName());

    @NotNull
    final ZMQ.Context context;

    @NotNull
    final ZMQ.Socket nodeStateReader;

    @NotNull
    final ZMQ.Socket nodeStateWriter;

    @NotNull
    final ZMQ.Socket journalReader;

    @NotNull
    final ZMQ.Socket journalWriter;

    @NotNull
    final Cache<String, NodeState> nodeStateCache;

    public ZeroMQNodeStore() {

        context = ZMQ.context(1);

        nodeStateReader = context.socket(ZMQ.REQ);
        nodeStateReader.connect("tcp://localhost:10000");

        nodeStateWriter = context.socket(ZMQ.REQ);
        nodeStateWriter.connect("tcp://localhost:10001");

        journalReader = context.socket(ZMQ.REQ);
        journalReader.connect("tcp://localhost:9001");

        journalWriter = context.socket(ZMQ.REQ);
        journalWriter.connect("tcp://localhost:9000");

        nodeStateCache = CacheBuilder.newBuilder()
            .maximumSize(1000).build();
    }

    @Override
    public NodeState getRoot() {
        byte[] msg;
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
        return newZeroMQNodeState(UUID.fromString(sMsg));
    }

    @Override
    public NodeState merge(NodeBuilder builder, CommitHook commitHook, CommitInfo info) throws CommitFailedException {
        final NodeState before = builder.getBaseState();
        final NodeState after = builder.getNodeState();
        final LoggingHook serialiser = LoggingHook.newLoggingHook(this::write);
        after.compareAgainstBaseState(before, serialiser);
        return after;
    }

    private void write(String s) {
        while (true) {
            final byte[] msg;
            try {
                synchronized (nodeStateWriter) {
                    nodeStateWriter.send(s);
                    msg = nodeStateWriter.recv(); // wait for confirmation
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
    }

    @Override
    public NodeState rebase(@NotNull NodeBuilder builder) {
        final NodeState root = getRoot();
        final NodeState before = builder.getBaseState();
        final NodeState after = builder.getNodeState();
        if (root.equals(before)) {
            return after;
        } else {
            final NodeBuilder rootBuilder = root.builder();
            after.compareAgainstBaseState(before, new ConflictAnnotatingRebaseDiff(rootBuilder));
            return rootBuilder.getNodeState();
        }
    }

    @Override
    public NodeState reset(NodeBuilder builder) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Blob createBlob(InputStream inputStream) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Blob getBlob(String reference) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String checkpoint(long lifetime, Map<String, String> properties) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String checkpoint(long lifetime) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, String> checkpointInfo(String checkpoint) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Iterable<String> checkpoints() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public NodeState retrieve(String checkpoint) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean release(String checkpoint) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
