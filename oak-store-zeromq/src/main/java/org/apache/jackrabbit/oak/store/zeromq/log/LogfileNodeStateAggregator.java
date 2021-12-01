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
import org.apache.jackrabbit.oak.store.zeromq.SimpleRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class LogfileNodeStateAggregator extends AbstractNodeStateAggregator {

    private static final Logger log = LoggerFactory.getLogger(LogfileNodeStateAggregator.class);

    private final LineReader reader;

    public LogfileNodeStateAggregator(String filePath, String blobCacheDir, String journalUrl) throws IOException {
        super(new File(blobCacheDir));
        caughtup = false;
        recordHandler = new SimpleRecordHandler(this.blobCacheDir, journalUrl);
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
        while (!shutDown) {
            String line = nextRecord();
            if (line == null) {
                if (!caughtup) {
                    log.info("We have caught up!");
                    caughtup = true;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
                continue;
            }
            if ("".equals(line)) {
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
            } catch(RuntimeException e) {
                throw e;
            }
        }
    }
}
