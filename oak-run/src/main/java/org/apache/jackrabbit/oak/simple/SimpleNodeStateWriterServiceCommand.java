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

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.store.zeromq.SimpleBlobStore;
import org.apache.jackrabbit.oak.store.zeromq.SimpleNodeStateWriterService;

import java.io.File;
import java.net.URI;
import java.util.List;

public class SimpleNodeStateWriterServiceCommand implements Command {

    public static final String NAME = "simple-nodestate-writer-service";

    private static final String summary = "Serves the contents of a simple blobstore\n" +
        "java -jar oak-run.jar " + NAME + " <simple-store-url> <sending-url> <receiving-url>" +
        "Example:\n" + NAME + " /tmp/imported/log.txt tcp://localhost:8000 tcp://localhost:8001";

    @Override
    public void execute(String... args) throws Exception {
        final OptionParser parser = new OptionParser();

        final Options opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(summary);

        final OptionSet optionSet = opts.parseAndConfigure(parser, args);
        final List<?> uris = optionSet.nonOptionArguments();

        if (uris.size() != 3) {
            throw new IllegalArgumentException(summary);
        }

        final CommonOptions commonOptions = opts.getOptionBean(CommonOptions.class);
        final File blobDir = new File(commonOptions.getURI(0).getPath());
        final String publisherUri = commonOptions.getURI(1).toString();
        final String subscriberUri = commonOptions.getURI(2).toString();
        final SimpleNodeStateWriterService simpleNodeStateWriterService = new SimpleNodeStateWriterService(blobDir, publisherUri, subscriberUri);
        simpleNodeStateWriterService.run();
   }
}