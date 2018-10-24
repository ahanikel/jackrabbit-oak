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

package org.apache.jackrabbit.oak.segment;

import java.util.ArrayList;
import java.util.List;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.segment.tool.ReaderLogger;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

public class NodeStateWrapper implements NodeState {

    private final String path;
    private final NodeState ns;
    private static final ReaderLogger LOG = ReaderLogger.newReaderLogger();
    private boolean isRootNode;

    public NodeStateWrapper(String path, NodeState ns, boolean isRootNode) {
        this.path = path;
        this.ns = ns;
        this.isRootNode = isRootNode;
    }

    public boolean isRootNode() {
        return isRootNode;
    }

    @Override
    public boolean exists() {
        return ns.exists();
    }

    @Override
    public boolean hasProperty(String name) {
        return ns.hasProperty(name);
    }

    @Override
    public PropertyState getProperty(String name) {
        LOG.propertyRead(path, name);
        return ns.getProperty(name);
    }

    @Override
    public boolean getBoolean(String name) {
        return ns.getBoolean(name);
    }

    @Override
    public long getLong(String name) {
        return ns.getLong(name);
    }

    @Override
    public String getString(String name) {
        return ns.getString(name);
    }

    @Override
    public Iterable<String> getStrings(String name) {
        return ns.getStrings(name);
    }

    @Override
    public String getName(String name) {
        return ns.getName(name);
    }

    @Override
    public Iterable<String> getNames(String name) {
        return ns.getNames(name);
    }

    @Override
    public long getPropertyCount() {
        return ns.getPropertyCount();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return ns.getProperties();
    }

    @Override
    public boolean hasChildNode(String name) {
        return ns.hasChildNode(name);
    }

    @Override
    public NodeState getChildNode(String name) throws IllegalArgumentException {
        final String newName = path + "/" + name;
        LOG.nodeRead(newName);
        return new NodeStateWrapper(newName, ns.getChildNode(name), false);
    }

    @Override
    public long getChildNodeCount(long max) {
        return ns.getChildNodeCount(max);
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return ns.getChildNodeNames();
    }

    public class ChildNodeEntryWrapper implements ChildNodeEntry {

        private final ChildNodeEntry entry;

        public ChildNodeEntryWrapper(ChildNodeEntry entry) {
            this.entry = entry;
        }

        @Override
        public String getName() {
            return entry.getName();
        }

        @Override
        public NodeState getNodeState() {
            return new NodeStateWrapper(String.join("/", path, entry.getName()), entry.getNodeState(), false);
        }
    }

    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        final List<ChildNodeEntry> entries = new ArrayList();
        ns.getChildNodeEntries().forEach(e -> entries.add(new ChildNodeEntryWrapper(e)));
        return entries;
    }

    @Override
    public NodeBuilder builder() {
        return ns.builder();
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        return ns.compareAgainstBaseState(base, diff);
    }
}
