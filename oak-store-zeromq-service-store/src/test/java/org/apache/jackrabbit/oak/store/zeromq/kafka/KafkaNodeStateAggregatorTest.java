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
package org.apache.jackrabbit.oak.store.zeromq.kafka;

import com.google.common.io.Files;
import com.google.common.io.LineReader;
import org.apache.jackrabbit.oak.store.zeromq.RecordHandler;
import org.apache.jackrabbit.oak.store.zeromq.SimpleRecordHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.StringTokenizer;

public class KafkaNodeStateAggregatorTest {

    public static String[][] consumerRecords = new String[][]{
            {"store1-thread1", "b64+", "8b3235b9ec9e796e0d343dded5f617a3"},
            {"store1-thread1", "b64d", "aGVsbG8gd29ybGQK"},
            {"store1-thread1", "b64!", ""},
            {"store1-thread1", "R:", "12da6a2a-ec9f-e7dc-c9c1-8b52ced67e5f 5dbc3e8d-b6d6-f7d0-6af3-102ecf99eb0c"},
            {"store1-thread1", "n^", "root a6b4b705-8856-df66-dce4-401b188653bd 00000000-0000-0000-0000-000000000000"},
            {"store1-thread1", "n+", ":clusterConfig 617f9357-5dc5-0f26-8e80-ffef5c938022"},
            {"store1-thread1", "p+", ":clusterId <STRING> = 520fef06-50ce-4e5f-8ce1-9ae47d515322"},
            {"store1-thread1", "p+", "testblob <BINARY> = 8b3235b9ec9e796e0d343dded5f617a3"},
            {"store1-thread1", "n!", ""},
            {"store1-thread1", "n!", ""},
            {"store1-thread1", "R!", ""},
            {"store1-thread1", "journal", "golden 12da6a2a-ec9f-e7dc-c9c1-8b52ced67e5f"}
    };

    @Before
    public void before() {
    }

    @Test
    public void testAggregator() throws IOException {
        final File blobCacheDir = Files.createTempDir();
        RecordHandler recordHandler = new SimpleRecordHandler(blobCacheDir, null);
        for (String[] rec : consumerRecords) {
            recordHandler.handleRecord(rec[0], rec[1], rec[2]);
        }
        Assert.assertEquals("TODO", recordHandler.readNodeState("617f9357-5dc5-0f26-8e80-ffef5c938022"));
        final byte[] hello = new byte[12];
        final InputStream blobIs = recordHandler
                .getBlob("8b3235b9ec9e796e0d343dded5f617a3")
                .getNewStream();
        int bytesRead;
        try {
            bytesRead = blobIs.read(hello);
        } catch (IOException e) {
            bytesRead = 0;
        }
        Assert.assertEquals(12, bytesRead);
        Assert.assertEquals("hello world", new String(Arrays.copyOf(hello, 11)));
    }

    @Test
    @Ignore("Just for manual debugging")
    public void testDebug() throws IOException {
        final File blobCacheDir = Files.createTempDir();
        final RecordHandler recordHandler = new SimpleRecordHandler(blobCacheDir, null);
        //final LineReader r = new LineReader(new FileReader("/var/folders/nr/scf5thc9157cz730xynsnh140000gp/T/logFile-1605835284401988811.log"));
        final LineReader r = new LineReader(new FileReader("/tmp/quickstart.log"));
        for (String line = r.readLine(); line != null; line = r.readLine()) {
            final StringTokenizer st = new StringTokenizer(line, " ");
            final String uuThreadId = st.nextToken();
            final String op = st.hasMoreTokens() ? st.nextToken() : "";
            final String args = st.hasMoreTokens() ? st.nextToken("\n") : "";
            recordHandler.handleRecord(uuThreadId, op, args);
        }
    }
}
