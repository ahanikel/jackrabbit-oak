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

import org.apache.jackrabbit.oak.api.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class NodeStateAggregatorTest {

    public static String[][] consumerRecords = new String[][]{
            {"b64+", "8b3235b9ec9e796e0d343dded5f617a3"},
            {"b64d", "aGVsbG8gd29ybGQK"},
            {"b64!", ""},
            {"R:", "12da6a2a-ec9f-e7dc-c9c1-8b52ced67e5f 5dbc3e8d-b6d6-f7d0-6af3-102ecf99eb0c"},
            {"n^", "root a6b4b705-8856-df66-dce4-401b188653bd 00000000-0000-0000-0000-000000000000"},
            {"n+", ":clusterConfig 617f9357-5dc5-0f26-8e80-ffef5c938022"},
            {"p+", ":clusterId <STRING> = 520fef06-50ce-4e5f-8ce1-9ae47d515322"},
            {"p+", "testblob <BINARY> = 8b3235b9ec9e796e0d343dded5f617a3"},
            {"n!", ""},
            {"n!", ""},
            {"R!", ""},
            {"journal", "golden 12da6a2a-ec9f-e7dc-c9c1-8b52ced67e5f"}
    };

    @Before
    public void before() {
    }

    @Test
    public void testAggregator() {
        NodeStateAggregator nodeStateAggregator = new NodeStateAggregator("golden");
        for (String[] rec : consumerRecords) {
            nodeStateAggregator.handleRecord(rec[0], rec[1]);
        }
        Assert.assertEquals("520fef06-50ce-4e5f-8ce1-9ae47d515322",
                nodeStateAggregator
                        .readNodeState("617f9357-5dc5-0f26-8e80-ffef5c938022")
                        .getProperty(":clusterId")
                        .getValue(Type.STRING));
        final byte[] hello = new byte[12];
        final InputStream blobIs = nodeStateAggregator
                .readNodeState("617f9357-5dc5-0f26-8e80-ffef5c938022")
                .getProperty("testblob")
                .getValue(Type.BINARY)
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
}
