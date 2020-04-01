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
import java.io.PrintWriter;

import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.segment.LoggingHook;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentIdProvider;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.tooling.BasicReadOnlyBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class SendCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();

        OptionSpec<File> segmentDir = parser.accepts("path", "the path to the segment store")
            .withRequiredArg()
            .ofType(File.class);

        OptionSpec<String> interval = parser.accepts ("interval", "the recordid range to send")
            .withOptionalArg()
            .ofType(String.class);
        
        OptionSet options = parser.parse(args);

        File path = segmentDir.value(options);
        String[] tokens = interval.value(options).trim().split("\\.\\.");

        if (tokens.length != 2) {
            System.err.println("Error parsing revision interval '" + interval + "'.");
            return;
        }

        try (ReadOnlyFileStore store = FileStoreBuilder.fileStoreBuilder(path).withBlobStore(new BasicReadOnlyBlobStore()).buildReadOnly()) {
            SegmentIdProvider idProvider = store.getSegmentIdProvider();
            RecordId idL;

            try {
                if (tokens[0].equalsIgnoreCase("head")) {
                    idL = store.getRevisions().getHead();
                } else if (tokens[0].equalsIgnoreCase("null")) {
                    idL = RecordId.NULL;
                } else {
                    idL = RecordId.fromString(idProvider, tokens[0]);
                }
            } catch (IllegalArgumentException e) {
                System.err.println("Invalid left endpoint for interval " + interval);
                return;
            }

            RecordId idR;

            try {
                if (tokens[1].equalsIgnoreCase("head")) {
                    idR = store.getRevisions().getHead();
                } else {
                    idR = RecordId.fromString(idProvider, tokens[1]);
                }
            } catch (IllegalArgumentException e) {
                System.err.println("Invalid left endpoint for interval " + interval);
                return;
            }

            try (PrintWriter pw = new PrintWriter (System.out)) {
                send(store, idL, idR, pw);
            }
        }
    }

    private boolean send(ReadOnlyFileStore store, RecordId idL, RecordId idR, PrintWriter pw) {
        try {
            final NodeState before = RecordId.NULL.equals(idL)
                ? EmptyNodeState.EMPTY_NODE
                : store.getReader().readNode(idL).getChildNode("root");
            final NodeState after = store.getReader().readNode(idR).getChildNode("root");
            after.compareAgainstBaseState(before, LoggingHook.newLoggingHook(pw::println));
            pw.println("n!");
            return true;
        } catch (SegmentNotFoundException ex) {
            System.err.println(ex.getMessage());
            pw.println("#SNFE " + ex.getSegmentId());
            return false;
        }
    }
}
