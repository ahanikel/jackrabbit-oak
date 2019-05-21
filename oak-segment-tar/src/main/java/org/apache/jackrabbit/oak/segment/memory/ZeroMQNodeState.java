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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class ZeroMQNodeState extends AbstractNodeState {

    private final UUID uuid;

    private final Map<String, UUID> children;

    private final Map<String, ZeroMQPropertyState> properties;

    public static ZeroMQNodeState newZeroMQNodeState(UUID uuid) {
        return new ZeroMQNodeState(uuid);
    }

    private ZeroMQNodeState(UUID uuid) {
        this.uuid = uuid;
        this.children = new HashMap<>();
        this.properties = new HashMap<>();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ZeroMQNodeState) {
            return this.uuid.equals(((ZeroMQNodeState) other).uuid);
        }
        return false;
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return properties.values();
    }

    @Override
    public boolean hasChildNode(String name) {
        return children.containsKey(name);
    }

    @Override
    public NodeState getChildNode(String name) throws IllegalArgumentException {
        if (children.containsKey(name)) {
            return new ZeroMQNodeState(children.get(name));
        }
        throw new IllegalArgumentException("No such child");
    }

    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return () -> {
            Stream s = children.entrySet().stream().map(child -> new ChildNodeEntry() {
                @Override
                public String getName() {
                    return child.getKey();
                }

                @Override
                public NodeState getNodeState() {
                    return ZeroMQNodeState.this.getChildNode(child.getKey());
                }
            });
            return s.iterator();
        };
    }

    @Override
    public NodeBuilder builder() {
        return new MemoryNodeBuilder(this);
    }
}
