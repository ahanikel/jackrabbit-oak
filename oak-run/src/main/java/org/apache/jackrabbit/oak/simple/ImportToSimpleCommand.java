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

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;

public class ImportToSimpleCommand implements Command {
    public static final String NAME = "import-to-simple";

    private final String summary = "Imports the contents of a NodeStore to a SimpleNodeStateStore\n" +
        "Example:\n" +
        "import-to-simple --fds-path /Users/axel/Downloads/crx-quickstart-6.4.0/repository/datastore segment:/Users/axel/Downloads/crx-quickstart-6.4.0/repository/segmentstore simple:///tmp/imported";

    @Override
    public void execute(String... args) throws Exception {
        final Stopwatch w = Stopwatch.createStarted();
        final OptionParser parser = new OptionParser();

        final Options opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(summary);

        final OptionSet optionSet = opts.parseAndConfigure(parser, args);
        final List<?> nodeStoreURIs = optionSet.nonOptionArguments();

        if (nodeStoreURIs.size() != 2) {
            throw new IllegalArgumentException(summary);
        }

        final CommonOptions commonOptions = opts.getOptionBean(CommonOptions.class);
        final URI source = commonOptions.getURI(0);
        final URI dest = commonOptions.getURI(1);

        if (!dest.getScheme().equals("simple")) {
            throw new IllegalArgumentException(summary);
        }

        final File destPath = new File(dest.getPath());
        if (destPath.exists()) {
            throw new IllegalArgumentException("Refusing to overwrite existing path.");
        } else {
            destPath.mkdirs();
        }
        final SimpleBlobStore blobStore = new SimpleBlobStore(destPath);
        final SimpleNodeStateStore simple = new SimpleNodeStateStore(blobStore);
        final NodeStore sourceNodeStore = NodeStoreFixtureProvider.create(source, opts, true).getStore();
        System.err.println("Nodestores are open, starting import.");
        final NodeStore memoryStore = new MemoryNodeStore();
        NodeBuilder builder = memoryStore.getRoot().builder();
        builder.setChildNode("root", sourceNodeStore.getRoot());
        final NodeState newSuperRoot = builder.getNodeState();
        final String newHead = simple.putNodeState(newSuperRoot).getRef();
        try (OutputStream journalFile = new FileOutputStream(new File(destPath, "journal-golden"))) {
            IOUtils.writeString(journalFile, newHead);
        }
        System.err.println("New root is " + newHead);
        System.err.println("Import finished successfully.");
    }
}