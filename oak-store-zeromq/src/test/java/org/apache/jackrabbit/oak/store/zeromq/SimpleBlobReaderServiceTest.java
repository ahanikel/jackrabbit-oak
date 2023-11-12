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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.jackrabbit.oak.commons.Buffer;
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
        long msgid = 123L;
        sendReadRequestString(msgid, "journal", "/etc/password");
        SimpleBlobReaderService.handleReaderService(reply, simpleBlobStore);
        assertEquals(msgid, Util.longFromBytes(request.recv()));
        assertEquals(0, Util.longFromBytes(request.recv()));
        assertEquals("F", request.recvStr());
        assertEquals("IllegalArgument", request.recvStr());

        sendReadRequestString(++msgid, "journal", "../../../../../../../../../../../../../../../../../../../../etc/passwd");
        SimpleBlobReaderService.handleReaderService(reply, simpleBlobStore);
        assertEquals(msgid, Util.longFromBytes(request.recv()));
        assertEquals(0, Util.longFromBytes(request.recv()));
        assertEquals("F", request.recvStr());
        assertEquals("IllegalArgument", request.recvStr());

        sendReadRequestString(++msgid, "journal", "golden");
        SimpleBlobReaderService.handleReaderService(reply, simpleBlobStore);
        assertEquals(msgid, Util.longFromBytes(request.recv()));
        assertEquals(0, Util.longFromBytes(request.recv()));
        assertEquals("E", request.recvStr());
        assertEquals(new UUID(0, 0).toString(), request.recvStr());

        sendReadRequestString(++msgid, "journal", "mytestjournal");
        SimpleBlobReaderService.handleReaderService(reply, simpleBlobStore);
        assertEquals(msgid, Util.longFromBytes(request.recv()));
        assertEquals(0, Util.longFromBytes(request.recv()));
        assertEquals("E", request.recvStr());
        assertEquals(TestUtils.testNodeStateHash, request.recvStr());
    }

    @Test
    public void handleReaderServiceHasBlob() {
        long msgid = 456;
        sendReadRequestString(msgid, "hasblob", "/etc/passwd");
        SimpleBlobReaderService.handleReaderService(reply, simpleBlobStore);
        assertEquals(msgid, Util.longFromBytes(request.recv()));
        assertEquals(0, Util.longFromBytes(request.recv()));
        assertEquals("F", request.recvStr());
        assertEquals("IllegalArgument", request.recvStr());

        sendReadRequestString(++msgid, "hasblob", "../../../../../../../../../../../../../../../../../../../../etc/passwd");
        SimpleBlobReaderService.handleReaderService(reply, simpleBlobStore);
        assertEquals(msgid, Util.longFromBytes(request.recv()));
        assertEquals(0, Util.longFromBytes(request.recv()));
        assertEquals("F", request.recvStr());
        assertEquals("IllegalArgument", request.recvStr());

        sendReadRequestString(++msgid, "hasblob", "0");
        SimpleBlobReaderService.handleReaderService(reply, simpleBlobStore);
        assertEquals(msgid, Util.longFromBytes(request.recv()));
        assertEquals(0, Util.longFromBytes(request.recv()));
        assertEquals("E", request.recvStr());
        assertEquals("false", request.recvStr());

        sendReadRequestString(++msgid, "hasblob", new UUID(0, 0).toString());
        SimpleBlobReaderService.handleReaderService(reply, simpleBlobStore);
        assertEquals(msgid, Util.longFromBytes(request.recv()));
        assertEquals(0, Util.longFromBytes(request.recv()));
        assertEquals("E", request.recvStr());
        assertEquals("false", request.recvStr());

        sendReadRequestString(++msgid, "hasblob", TestUtils.testNodeStateHash);
        SimpleBlobReaderService.handleReaderService(reply, simpleBlobStore);
        assertEquals(msgid, Util.longFromBytes(request.recv()));
        assertEquals(0, Util.longFromBytes(request.recv()));
        assertEquals("E", request.recvStr());
        assertEquals("true", request.recvStr());
    }

    @Test
    public void handleReaderServiceBlob() {
        long msgid = 789L;
        sendReadRequestString(msgid, "blob", "/etc/passwd");
        SimpleBlobReaderService.handleReaderService(reply, simpleBlobStore);
        assertEquals(msgid, Util.longFromBytes(request.recv()));
        assertEquals(0, Util.longFromBytes(request.recv()));
        assertEquals("F", request.recvStr());
        assertEquals("IllegalArgument", request.recvStr());

        sendReadRequestString(++msgid, "blob", "../../../../../../../../../../../../../../../../../../../../etc/passwd");
        SimpleBlobReaderService.handleReaderService(reply, simpleBlobStore);
        assertEquals(msgid, Util.longFromBytes(request.recv()));
        assertEquals(0, Util.longFromBytes(request.recv()));
        assertEquals("F", request.recvStr());
        assertEquals("IllegalArgument", request.recvStr());

        sendReadRequestString(++msgid, "blob", "0");
        SimpleBlobReaderService.handleReaderService(reply, simpleBlobStore);
        assertEquals(msgid, Util.longFromBytes(request.recv()));
        assertEquals(0, Util.longFromBytes(request.recv()));
        assertEquals("N", request.recvStr());
        assertEquals("", request.recvStr());

        sendReadRequestString(++msgid, "blob", new UUID(0, 0).toString());
        SimpleBlobReaderService.handleReaderService(reply, simpleBlobStore);
        assertEquals(msgid, Util.longFromBytes(request.recv()));
        assertEquals(0, Util.longFromBytes(request.recv()));
        assertEquals("N", request.recvStr());
        assertEquals("", request.recvStr());

        sendReadRequestString(++msgid, "blob", DigestUtils.md5Hex("someArbitraryData").toUpperCase());
        SimpleBlobReaderService.handleReaderService(reply, simpleBlobStore);
        assertEquals(msgid, Util.longFromBytes(request.recv()));
        assertEquals(0, Util.longFromBytes(request.recv()));
        assertEquals("N", request.recvStr());
        assertEquals("", request.recvStr());

        sendReadRequestString(++msgid, "blob", TestUtils.testNodeStateHash);
        SimpleBlobReaderService.handleReaderService(reply, simpleBlobStore);
        assertEquals(msgid, Util.longFromBytes(request.recv()));
        assertEquals(0, Util.longFromBytes(request.recv()));
        assertEquals("E", request.recvStr());
        assertEquals(TestUtils.testNodeState, request.recvStr());
    }

    @Test
    public void readLargeBlob() throws IOException {
        final int blobSize = 1_000_000_000;
        InputStream largeBlob = TestUtils.getLargeBlobInputStream(blobSize);
        String ref;
        try {
            ref = simpleBlobStore.putInputStream(largeBlob);
        } catch (BlobAlreadyExistsException e) {
            ref = e.getRef();
        }
        for (int count = 0; count < blobSize;) {
            sendReadRequestString(999, "blob", ref + " " + count + " -1");
            SimpleBlobReaderService.handleReaderService(reply, simpleBlobStore);
            assertEquals(999, Util.longFromBytes(request.recv()));
            assertEquals(0, Util.longFromBytes(request.recv()));
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

    private void sendReadRequestString(long msgid, String op, String msg) {
        Buffer b = Buffer.allocate(Long.BYTES);
        b.putLong(msgid);
        request.sendMore(b.array());
        request.sendMore(op);
        if (msg == null) {
            request.send("");
        } else {
            request.send(msg);
        }
    }
}