/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.run;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.tool.Replayer;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class ReceiveCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();

        OptionSpec<File> path = parser.accepts("path", "the path to the segment store")
                .withRequiredArg()
                .ofType(File.class);

        OptionSet options = parser.parse(args);
        receive(path.value(options));
    }

    private void receive(File path) throws IOException, InvalidFileStoreVersionException {
        final FileStore fs = FileStoreBuilder.fileStoreBuilder(path).build();
        final NodeStore store = SegmentNodeStoreBuilders.builder(fs).build();
        final Replayer replayer = new Replayer(store);
        replayer.setInputStream(System.in);
        replayer.run();
        fs.close();
    }
}
