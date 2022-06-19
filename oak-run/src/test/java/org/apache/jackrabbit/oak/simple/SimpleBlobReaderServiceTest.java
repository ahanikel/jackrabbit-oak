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
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        simpleRecordHandler.handleRecord("thread-1", 123, "b64+", TestUtils.testNodeStateHash.getBytes());
        simpleRecordHandler.handleRecord("thread-1", 124, "b64d", TestUtils.testNodeStateEncoded.getBytes());
        simpleRecordHandler.handleRecord("thread-1", 125, "b64!", "".getBytes());
        simpleRecordHandler.handleRecord("thread-1", 126, "journal", ("mytestjournal " + TestUtils.testNodeStateHash + " " + new UUID(0, 0).toString()).getBytes());
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
        assertEquals(TestUtils.testNodeStateHash, request.recvStr());
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

        request.send("hasblob " + TestUtils.testNodeStateHash);
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

        request.send("blob " + TestUtils.testNodeStateHash);
        simpleBlobReaderService.handleReaderService(reply);
        assertEquals("E", request.recvStr());
        assertEquals(TestUtils.testNodeState, request.recvStr());
    }

    @Test
    public void readLargeBlob() throws IOException {
        final int blobSize = 1_000_000_000;
        InputStream largeBlob = TestUtils.getLargeBlobInputStream(blobSize);
        String ref = simpleBlobStore.putInputStream(largeBlob);
        for (int count = 0; count < blobSize;) {
            request.send("blob " + ref + " " + count + " -1");
            simpleBlobReaderService.handleReaderService(reply);
            String code = request.recvStr();
            byte[] chunk = request.recv();
            assertEquals(count % 10, chunk[0]);
            count += chunk.length;
            if (count < blobSize) {
                assertEquals("C", code);
                assertEquals(1048576, chunk.length);
            } else {
                assertEquals("E", code);
                assertEquals(blobSize, count);
            }
        }
    }
}