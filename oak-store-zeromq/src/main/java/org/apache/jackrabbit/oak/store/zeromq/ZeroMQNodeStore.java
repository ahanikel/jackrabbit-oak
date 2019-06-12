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
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.ServiceScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static org.apache.jackrabbit.oak.store.zeromq.ZeroMQEmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.store.zeromq.ZeroMQNodeState.newZeroMQNodeState;

/**
 * A store which dumps everything into a queue.
 */
@Component(scope = ServiceScope.SINGLETON, immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
@Service
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
        nodeStateReader.connect("tcp://localhost:8000");

        nodeStateWriter = context.socket(ZMQ.REQ);
        nodeStateWriter.connect("tcp://localhost:8001");

        journalReader = context.socket(ZMQ.REQ);
        journalReader.connect("tcp://localhost:9000");

        journalWriter = context.socket(ZMQ.REQ);
        journalWriter.connect("tcp://localhost:9001");

        nodeStateCache = CacheBuilder.newBuilder()
            .maximumSize(1000).build();

        init();
    }

    public void init() {
        final String uuid = readRoot();
        if ("undefined".equals(uuid)) {
            final NodeBuilder builder = EMPTY_NODE(this::read, this::write).builder();
            builder.setChildNode("roots").setChildNode("1");
            try {
                merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            } catch (CommitFailedException e) {
                // never happens
            }
        }
    }

    private String readRoot() {
        String msg;
        while (true) {
            try {
                journalReader.send("ping");
                msg = journalReader.recvStr();
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
        String uuid = readRoot();
        return newZeroMQNodeState(uuid, this::read, this::write);
    }

    private void setRoot(String uuid) {
        while (true) {
            try {
                journalWriter.send(uuid);
            } catch (Throwable t) {
                log.warn(t.toString());
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    @Override
    public NodeState merge(NodeBuilder builder, CommitHook commitHook, CommitInfo info) throws CommitFailedException {
        final NodeState after = builder.getNodeState();
        setRoot(((ZeroMQNodeState) after).getUuid());
        return after;
    }

    private String read(String s) {
        String msg;
        while (true) {
            try {
                synchronized (nodeStateReader) {
                    nodeStateReader.send(s);
                    msg = nodeStateReader.recvStr();
                }
                log.info(msg);
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

    private void write(String s) {
       String msg;
        while (true) {
            try {
                synchronized (nodeStateWriter) {
                    nodeStateWriter.send(s);
                    msg = nodeStateWriter.recvStr(); // wait for confirmation
                }
                log.info(msg);
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
