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
package org.apache.jackrabbit.oak.store.zeromq.log;

import org.apache.jackrabbit.oak.store.zeromq.ZeroMQBackendStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A store used for in-memory operations.
 */
public class LogBackendStore extends ZeroMQBackendStore {

    private static final Logger log = LoggerFactory.getLogger(LogBackendStore.class);
    private final OutputStream logOut;

    public LogBackendStore(String instance, String logFile) throws FileNotFoundException {
        super(new LogfileNodeStateAggregator(instance, logFile));
        logOut = new FileOutputStream(logFile, true);
        setEventWriter(this::writeEvent);
        open();
    }

    public void finalize() {
        close();
    }

    private void writeEvent(String event) {
        for (int i = 0; ; ++i) {
            try {
                logOut.write(event.getBytes());
                break;
            } catch (IOException e) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException interruptedException) {
                    break;
                }
                if (i % 600 == 0) {
                    log.error("An IOException occurred but I'll keep retrying every 100ms: {}", e.getMessage());
                }
            }
        }
    }

    @Override
    public void close() {
        super.close();
        try {
            logOut.close();
        } catch (IOException e) {
        }
    }
}