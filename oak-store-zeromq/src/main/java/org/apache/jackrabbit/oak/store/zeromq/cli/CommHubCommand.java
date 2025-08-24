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
package org.apache.jackrabbit.oak.store.zeromq.cli;

import org.apache.jackrabbit.oak.store.zeromq.Constants;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import picocli.CommandLine;

@CommandLine.Command(
        name = "comm-hub",
        mixinStandardHelpOptions = true,
description = "Run the backend comunication hub")
public class CommHubCommand implements Runnable {

  @CommandLine.Option(names = {"-s", "--sending-uri"}, description = "URI listening for messages (default: ${DEFAULT-VALUE})")
  String sendingUri = Constants.DEFAULT_BACKEND_WRITER_URL;

  @CommandLine.Option(names = {"-r", "--receiving-uri"}, description = "URI sending messages to (default: ${DEFAULT-VALUE})")
  String receivingUri = Constants.DEFAULT_BACKEND_READER_URL;

  @Override
  public void run() {
    try (
            ZContext ctx = new ZContext();
            ZMQ.Socket publisher = ctx.createSocket(SocketType.PUB);
            ZMQ.Socket subscriber = ctx.createSocket(SocketType.SUB)
    ) {
      publisher.bind(receivingUri);
      subscriber.subscribe("".getBytes());
      subscriber.bind(sendingUri);
      ZMQ.proxy(subscriber, publisher, null);
    }
  }
}
