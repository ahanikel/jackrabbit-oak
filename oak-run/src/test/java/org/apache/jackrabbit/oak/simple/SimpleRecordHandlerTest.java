/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.jackrabbit.oak.simple;

import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.store.zeromq.SimpleBlobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.*;

public class SimpleRecordHandlerTest {

    private static final String journalQueueURL = "ipc://journalQueue";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private ZContext context;
    private ZMQ.Socket journalPublisher;
    private ZMQ.Socket journalSubscriber;
    private File blobDir;
    private SimpleBlobStore simpleBlobStore;
    private SimpleRecordHandler simpleRecordHandler;

    @Before
    public void setup() throws IOException {
        context = new ZContext();
        journalPublisher = context.createSocket(SocketType.PUB);
        journalSubscriber = context.createSocket(SocketType.SUB);
        journalSubscriber.subscribe("");
        journalPublisher.bind(journalQueueURL);
        journalSubscriber.connect(journalQueueURL);
        blobDir = temporaryFolder.newFolder();
        simpleBlobStore = new SimpleBlobStore(blobDir);
        simpleRecordHandler = new SimpleRecordHandler(simpleBlobStore, journalPublisher);
    }

    @After
    public void shutdown() {
        context.close();
    }

    @Test
    public void handleRecordBasics() throws IOException {
        simpleRecordHandler.handleRecord("thread-1", 123, "n:", "9B26508DCB3614BD2A7E9CB8889D4C12 " + new UUID(0, 0).toString());
        simpleRecordHandler.handleRecord("thread-1", 124, "n+", "child1 1234568");
        simpleRecordHandler.handleRecord("thread-1", 125, "n!", "");

        File offset = new File(blobDir, "offset");
        assertTrue(offset.exists());
        assertTrue(offset.isFile());
        assertEquals(8, offset.length());

        File sub1 = new File(blobDir, "9B");
        assertTrue(sub1.exists());
        assertTrue(sub1.isDirectory());

        File sub2 = new File(sub1, "26");
        assertTrue(sub2.exists());
        assertTrue(sub2.isDirectory());

        File sub3 = new File(sub2, "50");
        assertTrue(sub3.exists());
        assertTrue(sub3.isDirectory());

        File blob = new File(sub3, "9B26508DCB3614BD2A7E9CB8889D4C12");
        assertTrue(blob.exists());
        assertTrue(blob.isFile());
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        IOUtils.copy(new FileInputStream(blob), bos);
        assertEquals(
                "n:\n" +
                "n+ child1 1234568\n" +
                "n!\n", new String(bos.toByteArray()));
    }

    @Test
    public void handleRecordProperties() throws IOException {
        simpleRecordHandler.handleRecord("thread-1", 123, "n:", "6F3E7018B987ED1260B6B843364261BA " + new UUID(0, 0).toString());
        simpleRecordHandler.handleRecord("thread-1", 124, "p+", "prop1 <LONG> 1234568");
        simpleRecordHandler.handleRecord("thread-1", 125, "n!", "");
        File blob = new File(blobDir, "6F/3E/70/6F3E7018B987ED1260B6B843364261BA");
        assertTrue(blob.exists());
        assertTrue(blob.isFile());
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        IOUtils.copy(new FileInputStream(blob), bos);
        assertEquals(
                "n:\n" +
                        "p+ prop1 <LONG> 1234568\n" +
                        "n!\n", new String(bos.toByteArray()));
    }
}