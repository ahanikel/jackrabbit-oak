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

import com.google.common.base.Stopwatch;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.jackrabbit.oak.exporter.NodeStateSerializer;
import org.apache.jackrabbit.oak.run.cli.BlobStoreFixture;
import org.apache.jackrabbit.oak.run.cli.BlobStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixtureProvider;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.List;

public class CopyCommand implements Command {
    public static final String NAME = "copy";

    private final String summary = "Copies the contents of a NodeStore to another one\n" +
            "Example:\n" +
            "copy --fds-path /Users/axel/Downloads/crx-quickstart-6.4.0/repository/datastore segment:/Users/axel/Downloads/crx-quickstart-6.4.0/repository/segmentstore zeromq://two?writeBackJournal=true&writeBackNodes=true&blobCacheDir=/tmp/blobCache";

    @Override
    public void execute(String... args) throws Exception {
        final Stopwatch w = Stopwatch.createStarted();
        final OptionParser parser = new OptionParser();

        final Options opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(summary);

        OptionSet optionSet = opts.parseAndConfigure(parser, args);
        List<?> nodeStoreURIs = optionSet.nonOptionArguments();

        if (nodeStoreURIs.size() != 2) {
            throw new IllegalArgumentException("You need to provide a source and destination nodestore URI.");
        }

        NodeStoreCopier nodeStoreCopier = new NodeStoreCopier(opts);
        nodeStoreCopier.run();
    }
}
