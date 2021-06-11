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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.FileOutputStream;
import java.io.IOException;

public class LogFileWriter implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(LogFileWriter.class);

    private final ZContext context;
    private final String urlIn;
    private final String logFileName;

    public LogFileWriter(String urlIn, String logFileName) {
        this.urlIn = urlIn;
        this.logFileName = logFileName;
        this.context = new ZContext();
    }

    @Override
    public void run() {
        try (final FileOutputStream logOut = new FileOutputStream(logFileName, true)) {
            final ZMQ.Socket in = context.createSocket(SocketType.REP);
            in.connect(urlIn);
            while (!Thread.currentThread().isInterrupted()) {
                String event = in.recvStr();
                for (int i = 0; ; ++i) {
                    try {
                        logOut.write(event.getBytes());
                        logOut.write(0x0a);
                        break;
                    } catch (IOException e) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException interruptedException) {
                            break;
                        }
                        if (i % 600 == 0) {
                            log.error("An IOException occurred but I'll keep retrying every 100ms: {}", e.getMessage()); // TODO: logging should go to a socket, too
                        }
                    }
                }
                in.send("");
            }
        } catch (IOException e) {
            log.error(e.getMessage()); // TODO: logging should go to a socket, too
        }
    }
}