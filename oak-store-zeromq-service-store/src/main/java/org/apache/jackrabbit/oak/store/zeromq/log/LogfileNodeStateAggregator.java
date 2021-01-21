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
import org.apache.jackrabbit.oak.store.zeromq.AbstractNodeStateAggregator;
import org.apache.jackrabbit.oak.store.zeromq.NodeStoreRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class LogfileNodeStateAggregator extends AbstractNodeStateAggregator {

    private static final Logger log = LoggerFactory.getLogger(LogfileNodeStateAggregator.class);

    private final LineReader reader;

    public LogfileNodeStateAggregator(String instance, String filePath) throws FileNotFoundException {
        caughtup = false;
        recordHandler = new NodeStoreRecordHandler(instance);
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
                    return null;
                }
            }
        }
    }

    @Override
    public void run() {
        while (true) {
            final String line = nextRecord();
            if (line == null) {
                log.info("We have caught up!");
                caughtup = true;
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
                break;
            }
            final int afterKey = line.indexOf(' ');
            if (afterKey >= 0) {
                recordHandler.handleRecord(line.substring(0, afterKey), line.substring(afterKey + 1));
            } else {
                recordHandler.handleRecord(line, "");
            }
        }
    }
}
