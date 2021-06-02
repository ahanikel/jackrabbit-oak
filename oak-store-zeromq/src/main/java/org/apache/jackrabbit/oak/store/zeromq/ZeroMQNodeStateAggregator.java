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
package org.apache.jackrabbit.oak.store.zeromq;

import com.google.common.io.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class ZeroMQNodeStateAggregator extends AbstractNodeStateAggregator {

    private static final Logger log = LoggerFactory.getLogger(ZeroMQNodeStateAggregator.class);

    private ZContext context;
    private ZMQ.Socket socket;

    public ZeroMQNodeStateAggregator(String urlIn, String urlOut) {
        context = new ZContext();
        socket = context.createSocket(SocketType.SUB);
        socket.subscribe("");
        socket.setReceiveTimeOut(1000);
        socket.connect(urlIn);
        caughtup = false;
        recordHandler = new FileSystemStoreHandler(urlOut);
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            String line = socket.recvStr();
            if (line == null) {
                if (!caughtup) {
                    log.info("We have caught up!");
                    caughtup = true;
                    socket.setReceiveTimeOut(-1);
                }
                continue;
            }
            if ("".equals(line)) {
                log.warn("Empty line");
                continue;
            }
            try {
                final String uuThreadId = line.substring(0, line.indexOf(" "));
                final String strippedLine = line.substring(line.indexOf(" ") + 1);
                final int afterKey = strippedLine.indexOf(' ');
                if (afterKey >= 0) {
                    recordHandler.handleRecord(uuThreadId, strippedLine.substring(0, afterKey), strippedLine.substring(afterKey + 1));
                } else {
                    recordHandler.handleRecord(uuThreadId, strippedLine, "");
                }
            } catch(Throwable t) {
                log.warn("Line: {}", line);
                log.warn(t.getMessage() + " - continuing anyway");
            }
        }
    }
}
