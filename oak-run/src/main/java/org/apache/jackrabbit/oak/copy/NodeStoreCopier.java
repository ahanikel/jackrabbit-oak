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

package org.apache.jackrabbit.oak.copy;

import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class NodeStoreCopier implements Runnable {

    private final CommonOptions commonOptions;
    private final Options options;

    public NodeStoreCopier(Options options) {
        this.options = options;
        this.commonOptions = options.getOptionBean(CommonOptions.class);
    }

    @Override
    public void run() {
        try {
            final NodeStore source = NodeStoreFixtureProvider.create(commonOptions.getURI(0), options, true).getStore();
            final NodeStore destination = NodeStoreFixtureProvider.create(commonOptions.getURI(1), options, false).getStore();
            final NodeState root = source.getRoot();
            final NodeBuilder builder = destination.getRoot().builder();
            final NodeStateDiff diff = new ApplyDiff(builder);
            root.compareAgainstBaseState(EmptyNodeState.EMPTY_NODE, diff);
            destination.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            System.out.println("Finished copying.");
            System.exit(0); // TODO: there are better ways to terminate background threads
        } catch (Exception e) {
            System.out.println("ERROR: " + e.getMessage());
        }
    }
}
