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

import org.apache.jackrabbit.oak.commons.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class LogFilePublisher implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(LogFilePublisher.class);

    private final ZContext context;
    private final String urlOut;
    private final String logFileName;

    public LogFilePublisher(String urlOut, String logFileName) {
        this.urlOut = urlOut;
        this.logFileName = logFileName;
        this.context = new ZContext();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try (final FileInputStream logIn = new FileInputStream(logFileName)) {
                final ZMQ.Socket out = context.createSocket(SocketType.REQ);
                out.connect(urlOut);
                while (!Thread.currentThread().isInterrupted()) {
                    String line = IOUtils.readString(logIn);
                    out.send(line);
                    out.recv();
                }
            } catch (IOException e) {
                log.error(e.getMessage()); // TODO: logging should go to a socket, too
                try {
                    Thread.sleep(100);
                } catch (InterruptedException interruptedException) {
                    break;
                }
            }
        }
    }
}