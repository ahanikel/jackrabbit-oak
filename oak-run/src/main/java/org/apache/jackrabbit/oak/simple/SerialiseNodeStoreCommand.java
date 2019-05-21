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
package org.apache.jackrabbit.oak.simple;

import com.google.common.base.Stopwatch;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.store.zeromq.SimpleBlobStore;
import org.apache.jackrabbit.oak.store.zeromq.SimpleNodeStateStore;
import org.apache.jackrabbit.oak.store.zeromq.SimpleNodeStoreSerialiser;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;

public class SerialiseNodeStoreCommand implements Command {
    public static final String NAME = "serialise-nodestore";

    private final String summary = "Serialises the NodeStore to stdout excluding blobs\n" +
        "Example:\n" +
        "serialise-nodestore --fds-path /Users/axel/Downloads/crx-quickstart-6.4.0/repository/datastore segment:/Users/axel/Downloads/crx-quickstart-6.4.0/repository/segmentstore";

    @Override
    public void execute(String... args) throws Exception {
        final Stopwatch w = Stopwatch.createStarted();
        final OptionParser parser = new OptionParser();

        final Options opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(summary);

        final OptionSet optionSet = opts.parseAndConfigure(parser, args);
        final List<?> nodeStoreURIs = optionSet.nonOptionArguments();

        if (nodeStoreURIs.size() != 1) {
            throw new IllegalArgumentException(summary);
        }

        final CommonOptions commonOptions = opts.getOptionBean(CommonOptions.class);
        final URI source = commonOptions.getURI(0);

        final NodeStore sourceNodeStore = NodeStoreFixtureProvider.create(source, opts, true).getStore();
        final SimpleNodeStoreSerialiser ser = new SimpleNodeStoreSerialiser(System.out::print);
        final NodeState ns = sourceNodeStore.getRoot().getChildNode("content");
        ser.run(ns, "/content");
    }
}