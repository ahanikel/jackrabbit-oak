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

import com.google.common.io.LineReader;
import org.apache.jackrabbit.oak.store.zeromq.RecordHandler;
import org.apache.jackrabbit.oak.store.zeromq.ZeroMQNodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class LogfileNodeStateAggregator implements org.apache.jackrabbit.oak.store.zeromq.NodeStateAggregator {

    private static final Logger log = LoggerFactory.getLogger(LogfileNodeStateAggregator.class);

    private final RecordHandler recordHandler;
    private final LineReader reader;
    private volatile boolean caughtup;

    public LogfileNodeStateAggregator(String instance, String filePath) throws FileNotFoundException {
        caughtup = false;
        recordHandler = new RecordHandler(instance);
        final InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(filePath));
        reader = new LineReader(inputStreamReader);
    }

    private String nextRecord() {
        while (true) {
            try {
                return reader.readLine();
            } catch (IOException e) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException interruptedException) {
                }
            }
        }
    }

    private StringTokenizer tokens(String value) {
        return new StringTokenizer(value);
    }

    @Override
    public void run() {
        while (true) {
            final String line = nextRecord();
            final int afterKey = line.indexOf(' ');
            if (afterKey >= 0) {
                recordHandler.handleRecord(line.substring(0, afterKey), line.substring(afterKey + 1));
            }
        }
    }

    @Override
    public boolean hasCaughtUp() {
        return caughtup;
    }

    @Override
    public ZeroMQNodeStore getNodeStore() {
        return recordHandler.getNodeStore();
    }

    @Override
    public String getJournalHead(String journalName) {
        final String ret = recordHandler.getJournalHead(journalName);
        if (ret == null) {
            return "undefined";
        }
        return ret;
    }
}
