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

public class SimpleNodeBuilder extends MemoryNodeBuilder {
    public SimpleNodeBuilder(@NotNull NodeState base) {
        super(base);
        assert(base instanceof SimpleNodeState);
    }

    public SimpleNodeBuilder(SimpleNodeBuilder parent, String name) {
        super(parent, name);
    }

    @Override
    public SimpleNodeState getNodeState() {
        final SimpleNodeState base = (SimpleNodeState) getBaseState();
        final SimpleNodeStateStore snss = new SimpleNodeStateStore(base.getStore());
        try {
            final String ref = snss.putNodeState(super.getNodeState());
            return SimpleNodeState.get(base.getStore(), ref);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public MemoryNodeBuilder createChildBuilder(String name) {
        return new SimpleNodeBuilder(this, name);
    }

    @Override
    public Blob createBlob(InputStream is) throws IOException {
        final SimpleNodeState base = (SimpleNodeState) getBaseState();
        final SimpleBlobStore store = base.getStore();
        final String ref = base.getStore().putInputStream(is);
        return new SimpleBlob(store, ref);
    }
}
