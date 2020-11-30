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

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;
import java.util.function.Function;

public class ZeroMQNodeBuilder extends MemoryNodeBuilder {

    @NotNull
    private final ZeroMQNodeStore ns;

    @NotNull
    private final Function<String, ZeroMQNodeState> reader;

    private final Consumer<ZeroMQNodeState> writer;

    ZeroMQNodeBuilder(
            @NotNull ZeroMQNodeStore ns,
            @NotNull ZeroMQNodeState base,
            Function<String, ZeroMQNodeState> reader,
            Consumer<ZeroMQNodeState> writer) {
        super(base);
        this.ns = ns;
        this.reader = reader;
        this.writer = writer;
    }

    private ZeroMQNodeBuilder(
            @NotNull ZeroMQNodeStore ns,
            @NotNull ZeroMQNodeBuilder parent,
            @NotNull String name
    ) {
        super(parent, name);
        this.ns = ns;
        this.reader = parent.reader;
        this.writer = parent.writer;
    }

    //-------------------------------------------------------< NodeBuilder >--

    @NotNull
    @Override
    public NodeState getNodeState() {
        return super.getNodeState();
    }

    @Override
    protected MemoryNodeBuilder createChildBuilder(String name) {
        return new ZeroMQNodeBuilder(this.ns, this, name);
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return ns.createBlob(stream);
    }

    @Override
    public void reset(NodeState newBase) {
        try {
            super.reset(newBase);
        } catch (IllegalStateException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
