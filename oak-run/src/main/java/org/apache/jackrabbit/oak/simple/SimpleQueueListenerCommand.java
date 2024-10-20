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
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.File;
import java.util.List;

public class SimpleQueueListenerCommand implements Command {

    public static final String NAME = "simple-queue-listener";

    private static final String summary = "Displays what goes on on the message queue\n" +
        "java -jar oak-run.jar " + NAME + " <queue-url>" +
        "Example:\n" + NAME + " tcp://localhost:8001";

    @Override
    public void execute(String... args) throws Exception {
        final Options opts = new Options();
        opts.setCommandName(NAME);
        opts.setSummary(summary);

        final OptionParser parser = new OptionParser();
        final OptionSet optionSet = opts.parseAndConfigure(parser, args);
        final List<?> uris = optionSet.nonOptionArguments();

        if (uris.size() != 1) {
            throw new IllegalArgumentException(summary);
        }

        final CommonOptions commonOptions = opts.getOptionBean(CommonOptions.class);
        final String subscriberUri = commonOptions.getURI(0).toString();

        final ZContext context = new ZContext();
        final ZMQ.Socket socket = context.createSocket(SocketType.SUB);
        socket.subscribe("");
        socket.connect(subscriberUri);

        while (!Thread.currentThread().isInterrupted()) {
            byte[] data = null;
            String s;
            try {
                data = socket.recv();
                if (data.length == Long.BYTES) {
                    System.out.print("" + Buffer.wrap(data).getLong() + " | ");
                    continue;
                }
                s = new String(data);
                if (s.contains("-re")) {
                    System.out.println();
                }
                System.out.print(s);
                System.out.print(" | ");
            } catch (Exception e) {
                if (data == null) {
                    System.out.println("ERROR: " + e.toString());
                } else {
                    System.out.print("<Binary data of size " + data.length + "> | ");
                }
            }
        }
    }
}