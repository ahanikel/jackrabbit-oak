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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

final class ZeroMQNodeStateDiffBuilder implements NodeStateDiff {

    private final Function<String, ZeroMQNodeState> reader;
    private final Consumer<ZeroMQNodeState> writer;

    private Map<String, String> children;
    private Map<String, ZeroMQPropertyState> properties;

    private final ZeroMQNodeStore ns;
    private final ZeroMQNodeState before;

    private boolean dirty;

    ZeroMQNodeStateDiffBuilder(ZeroMQNodeStore ns, ZeroMQNodeState before, Function<String, ZeroMQNodeState> reader, Consumer<ZeroMQNodeState> writer) {
        this.ns = ns;
        this.reader = reader;
        this.writer = writer;
        this.before = before;
        reset();
    }

    private void reset() {
        this.children = new HashMap<>(1000);
        this.properties = new HashMap<>(100);
        this.children.putAll(before.children);
        this.properties.putAll(before.properties);
        this.dirty = false;
    }

    public ZeroMQNodeState getNodeState() {
        if (dirty) {
            final ZeroMQNodeState ret = new ZeroMQNodeState(this.ns, this.children, this.properties, null, reader, writer);
            writer.accept(ret);
            return ret;
        } else {
            return before;
        }
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        properties.put(after.getName(), ZeroMQPropertyState.fromPropertyState(this.ns, after));
        dirty = true;
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        properties.remove(before.getName());
        return propertyAdded(after);
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        properties.remove(before.getName());
        dirty = true;
        return true;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        if (after instanceof ZeroMQNodeState) {
            this.children.put(name, ((ZeroMQNodeState) after).getUuid());
        } else {
            final ZeroMQNodeState before = ns.emptyNode;
            final ZeroMQNodeStateDiffBuilder diff = new ZeroMQNodeStateDiffBuilder(this.ns, before, reader, writer);
            after.compareAgainstBaseState(before, diff);
            final ZeroMQNodeState child = diff.getNodeState();
            this.children.put(name, child.getUuid());
        }
        dirty = true;
        return true;
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        this.children.remove(name);
        if (after instanceof ZeroMQNodeState) {
            this.children.put(name, ((ZeroMQNodeState) after).getUuid());
        } else {
            final ZeroMQNodeStateDiffBuilder diff = new ZeroMQNodeStateDiffBuilder(this.ns, (ZeroMQNodeState) before, reader, writer);
            after.compareAgainstBaseState(before, diff);
            final ZeroMQNodeState child = diff.getNodeState();
            this.children.put(name, child.getUuid());
        }
        dirty = true;
        return true;
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        this.children.remove(name);
        dirty = true;
        return true;
    }
}
