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
package org.apache.jackrabbit.oak.simple;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.jackrabbit.oak.store.zeromq.SimpleBlobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.File;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class SimpleBlobReaderServiceTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private File blobDir;
    private SimpleBlobStore simpleBlobStore;
    private SimpleBlobReaderService simpleBlobReaderService;
    private ZContext context;
    private ZMQ.Socket request;
    private ZMQ.Socket reply;
    private ZMQ.Socket journalPublisher;
    private SimpleRecordHandler simpleRecordHandler;

    @Before
    public void setup() throws Exception {
        context = new ZContext();

        request = context.createSocket(SocketType.REQ);
        reply = context.createSocket(SocketType.REP);
        reply.bind("inproc://testService");
        request.connect("inproc://testService");

        journalPublisher = context.createSocket(SocketType.PUB);
        journalPublisher.bind("inproc://testJournalQueue");

        blobDir = temporaryFolder.newFolder();
        simpleBlobStore = new SimpleBlobStore(blobDir);
        simpleBlobReaderService = new SimpleBlobReaderService(simpleBlobStore);

        simpleRecordHandler = new SimpleRecordHandler(simpleBlobStore, journalPublisher);
        simpleRecordHandler.handleRecord("thread-1", 123, "b64+", "CDBA3AE79386D3CF3DAAE8EC7F760588");
        simpleRecordHandler.handleRecord("thread-1", 124, "b64d", "bjoKbisgOmFzeW5jIDQzRDA3MjI5N0MwOTM3NUZBNjBGOUE2RDEzNzI0QkIzCm4rIDpjbHVzdGVyQ29uZmlnIEVFQkFCNTBCMkJBNEFFMzY5RjVGMTlGOTRFRkFDQzdECm4rIGFwcHMgQUNGMjQ3OTZBRjk3NTA1REFDMEYxMzU4Q0NENUVBNUMKbisgYmluIEM2RDREQjMxMjIxNjQ3RUQwQTlFMzVENTI0RjdCRDFBCm4rIGNvbmYgMUMwMUU3NUFEQTY3QkU3NURCNzhBNzg1MzA4MDMyN0EKbisgY29udGVudCA2NzFGODVCMUYwMUI5NEMwMDU3MzAzNDRFNEVGQTQ2MwpuKyBldGMgREU5OTMxNkExNTlCRDg4QkI5MDlFMERBOUQ0MDhBNzcKbisgaG9tZSBFQTYyMkZENDMwMzI3NTkyMEJCODQ5MjFEMEE5NkFGRApuKyBqY3I6c3lzdGVtIDgyOUVBRDdEQTlGQjFGRUNGRUM1MzUxMTM5MjhEODE1Cm4rIGxpYnMgMjZFMkRGMDM0NjRENjgzQzFDQjhDODczMEJBQUU3MEQKbisgb2FrOmluZGV4IDVBMjI1RDBFNTgwQkQ4QkNDMDIzRDJEMTQ5QjZGODBFCm4rIHJlcDpwb2xpY3kgQzVEMzA3RTkwQjBGQ0UxMDEyN0VFNzE4RjVDMzY2QzMKbisgcmVwOnJlcG9Qb2xpY3kgM0ExQzdFMTUxMUM1RUMyQjUyM0JCRkRDRTg5MEEwNkUKbisgc3lzdGVtIERDOEZDMTJFREUwMTU0MUUyQjg2N0MwNTYxMjE1OEQ1Cm4rIHRtcCAyRDczNEI0MTA1N0JENDQ3RTZFRkU5N0M1MEM4MUJBOApuKyB2YXIgMUJGRDMwNUM3QzcxMkFDMEVBOUJCQjVFODBBQUU4NzMKcCsgamNyOm1peGluVHlwZXMgPE5BTUVTPiBbcmVwOkFjY2Vzc0NvbnRyb2xsYWJsZSxyZXA6UmVwb0FjY2Vzc0NvbnRyb2xsYWJsZV0KcCsgamNyOnByaW1hcnlUeXBlIDxOQU1FPiByZXA6cm9vdApwKyBzbGluZzpyZXNvdXJjZVR5cGUgPFNUUklORz4gc2xpbmc6cmVkaXJlY3QKcCsgc2xpbmc6dGFyZ2V0IDxTVFJJTkc+IC9pbmRleC5odG1sCm4hCg==");
        simpleRecordHandler.handleRecord("thread-1", 125, "b64!", "");
        simpleRecordHandler.handleRecord("thread-1", 126, "journal", "mytestjournal CDBA3AE79386D3CF3DAAE8EC7F760588 " + new UUID(0, 0).toString());
    }

    @After
    public void shutdown() throws Exception {
        context.close();
    }

    @Test
    public void handleReaderServiceJournal() {
        request.send("journal /etc/passwd");
        simpleBlobReaderService.handleReaderService(reply);
        assertEquals("F", request.recvStr());
        assertEquals("IllegalArgument", request.recvStr());

        request.send("journal ../../../../../../../../../../../../../../../../../../../../etc/passwd");
        simpleBlobReaderService.handleReaderService(reply);
        assertEquals("F", request.recvStr());
        assertEquals("IllegalArgument", request.recvStr());

        request.send("journal golden");
        simpleBlobReaderService.handleReaderService(reply);
        assertEquals("E", request.recvStr());
        assertEquals(new UUID(0, 0).toString(), request.recvStr());

        request.send("journal mytestjournal");
        simpleBlobReaderService.handleReaderService(reply);
        assertEquals("E", request.recvStr());
        assertEquals("CDBA3AE79386D3CF3DAAE8EC7F760588", request.recvStr());
    }

    @Test
    public void handleReaderServiceHasBlob() {
        request.send("hasblob /etc/passwd");
        simpleBlobReaderService.handleReaderService(reply);
        assertEquals("F", request.recvStr());
        assertEquals("IllegalArgument", request.recvStr());

        request.send("hasblob ../../../../../../../../../../../../../../../../../../../../etc/passwd");
        simpleBlobReaderService.handleReaderService(reply);
        assertEquals("F", request.recvStr());
        assertEquals("IllegalArgument", request.recvStr());

        request.send("hasblob 0");
        simpleBlobReaderService.handleReaderService(reply);
        assertEquals("E", request.recvStr());
        assertEquals("false", request.recvStr());

        request.send("hasblob " + new UUID(0, 0).toString());
        simpleBlobReaderService.handleReaderService(reply);
        assertEquals("E", request.recvStr());
        assertEquals("false", request.recvStr());

        request.send("hasblob CDBA3AE79386D3CF3DAAE8EC7F760588");
        simpleBlobReaderService.handleReaderService(reply);
        assertEquals("E", request.recvStr());
        assertEquals("true", request.recvStr());
    }

    @Test
    public void handleReaderServiceBlob() {
        request.send("blob /etc/passwd");
        simpleBlobReaderService.handleReaderService(reply);
        assertEquals("F", request.recvStr());
        assertEquals("IllegalArgument", request.recvStr());

        request.send("blob ../../../../../../../../../../../../../../../../../../../../etc/passwd");
        simpleBlobReaderService.handleReaderService(reply);
        assertEquals("F", request.recvStr());
        assertEquals("IllegalArgument", request.recvStr());

        request.send("blob 0");
        simpleBlobReaderService.handleReaderService(reply);
        assertEquals("N", request.recvStr());
        assertEquals("", request.recvStr());

        request.send("blob " + new UUID(0, 0).toString());
        simpleBlobReaderService.handleReaderService(reply);
        assertEquals("N", request.recvStr());
        assertEquals("", request.recvStr());

        request.send("blob " + DigestUtils.md5Hex("someArbitraryData").toUpperCase());
        simpleBlobReaderService.handleReaderService(reply);
        assertEquals("N", request.recvStr());
        assertEquals("", request.recvStr());

        request.send("blob CDBA3AE79386D3CF3DAAE8EC7F760588");
        simpleBlobReaderService.handleReaderService(reply);
        assertEquals("E", request.recvStr());
        assertEquals(898, request.recvStr().length());
    }
}