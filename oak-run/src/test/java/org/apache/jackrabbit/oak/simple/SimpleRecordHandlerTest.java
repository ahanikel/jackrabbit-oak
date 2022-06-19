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
import java.io.InputStream;
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
        simpleRecordHandler.handleRecord("thread-1", 123, "n:", ("9B26508DCB3614BD2A7E9CB8889D4C12 " + new UUID(0, 0).toString()).getBytes());
        simpleRecordHandler.handleRecord("thread-1", 124, "n+", "child1 1234568".getBytes());
        simpleRecordHandler.handleRecord("thread-1", 125, "n!", "".getBytes());

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
        simpleRecordHandler.handleRecord("thread-1", 123, "n:", ("6F3E7018B987ED1260B6B843364261BA " + new UUID(0, 0).toString()).getBytes());
        simpleRecordHandler.handleRecord("thread-1", 124, "p+", "prop1 <LONG> 1234568".getBytes());
        simpleRecordHandler.handleRecord("thread-1", 125, "n!", "".getBytes());
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

    @Test
    public void handleRecordBlobs() throws IOException {
        simpleRecordHandler.handleRecord("thread-1", 123, "b64+", "CDBA3AE79386D3CF3DAAE8EC7F760588".getBytes());
        simpleRecordHandler.handleRecord("thread-1", 124, "b64d", "bjoKbisgOmFzeW5jIDQzRDA3MjI5N0MwOTM3NUZBNjBGOUE2RDEzNzI0QkIzCm4rIDpjbHVzdGVyQ29uZmlnIEVFQkFCNTBCMkJBNEFFMzY5RjVGMTlGOTRFRkFDQzdECm4rIGFwcHMgQUNGMjQ3OTZBRjk3NTA1REFDMEYxMzU4Q0NENUVBNUMKbisgYmluIEM2RDREQjMxMjIxNjQ3RUQwQTlFMzVENTI0RjdCRDFBCm4rIGNvbmYgMUMwMUU3NUFEQTY3QkU3NURCNzhBNzg1MzA4MDMyN0EKbisgY29udGVudCA2NzFGODVCMUYwMUI5NEMwMDU3MzAzNDRFNEVGQTQ2MwpuKyBldGMgREU5OTMxNkExNTlCRDg4QkI5MDlFMERBOUQ0MDhBNzcKbisgaG9tZSBFQTYyMkZENDMwMzI3NTkyMEJCODQ5MjFEMEE5NkFGRApuKyBqY3I6c3lzdGVtIDgyOUVBRDdEQTlGQjFGRUNGRUM1MzUxMTM5MjhEODE1Cm4rIGxpYnMgMjZFMkRGMDM0NjRENjgzQzFDQjhDODczMEJBQUU3MEQKbisgb2FrOmluZGV4IDVBMjI1RDBFNTgwQkQ4QkNDMDIzRDJEMTQ5QjZGODBFCm4rIHJlcDpwb2xpY3kgQzVEMzA3RTkwQjBGQ0UxMDEyN0VFNzE4RjVDMzY2QzMKbisgcmVwOnJlcG9Qb2xpY3kgM0ExQzdFMTUxMUM1RUMyQjUyM0JCRkRDRTg5MEEwNkUKbisgc3lzdGVtIERDOEZDMTJFREUwMTU0MUUyQjg2N0MwNTYxMjE1OEQ1Cm4rIHRtcCAyRDczNEI0MTA1N0JENDQ3RTZFRkU5N0M1MEM4MUJBOApuKyB2YXIgMUJGRDMwNUM3QzcxMkFDMEVBOUJCQjVFODBBQUU4NzMKcCsgamNyOm1peGluVHlwZXMgPE5BTUVTPiBbcmVwOkFjY2Vzc0NvbnRyb2xsYWJsZSxyZXA6UmVwb0FjY2Vzc0NvbnRyb2xsYWJsZV0KcCsgamNyOnByaW1hcnlUeXBlIDxOQU1FPiByZXA6cm9vdApwKyBzbGluZzpyZXNvdXJjZVR5cGUgPFNUUklORz4gc2xpbmc6cmVkaXJlY3QKcCsgc2xpbmc6dGFyZ2V0IDxTVFJJTkc+IC9pbmRleC5odG1sCm4hCg==".getBytes());
        simpleRecordHandler.handleRecord("thread-1", 125, "b64!", "".getBytes());
        File blob = new File(blobDir, "CD/BA/3A/CDBA3AE79386D3CF3DAAE8EC7F760588");
        assertTrue(blob.exists());
        assertTrue(blob.isFile());
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        IOUtils.copy(new FileInputStream(blob), bos);
        assertEquals("n:\n" +
                        "n+ :async 43D072297C09375FA60F9A6D13724BB3\n" +
                        "n+ :clusterConfig EEBAB50B2BA4AE369F5F19F94EFACC7D\n" +
                        "n+ apps ACF24796AF97505DAC0F1358CCD5EA5C\n" +
                        "n+ bin C6D4DB31221647ED0A9E35D524F7BD1A\n" +
                        "n+ conf 1C01E75ADA67BE75DB78A7853080327A\n" +
                        "n+ content 671F85B1F01B94C005730344E4EFA463\n" +
                        "n+ etc DE99316A159BD88BB909E0DA9D408A77\n" +
                        "n+ home EA622FD4303275920BB84921D0A96AFD\n" +
                        "n+ jcr:system 829EAD7DA9FB1FECFEC535113928D815\n" +
                        "n+ libs 26E2DF03464D683C1CB8C8730BAAE70D\n" +
                        "n+ oak:index 5A225D0E580BD8BCC023D2D149B6F80E\n" +
                        "n+ rep:policy C5D307E90B0FCE10127EE718F5C366C3\n" +
                        "n+ rep:repoPolicy 3A1C7E1511C5EC2B523BBFDCE890A06E\n" +
                        "n+ system DC8FC12EDE01541E2B867C05612158D5\n" +
                        "n+ tmp 2D734B41057BD447E6EFE97C50C81BA8\n" +
                        "n+ var 1BFD305C7C712AC0EA9BBB5E80AAE873\n" +
                        "p+ jcr:mixinTypes <NAMES> [rep:AccessControllable,rep:RepoAccessControllable]\n" +
                        "p+ jcr:primaryType <NAME> rep:root\n" +
                        "p+ sling:resourceType <STRING> sling:redirect\n" +
                        "p+ sling:target <STRING> /index.html\n" +
                        "n!\n",
                new String(bos.toByteArray()));
    }

    @Test
    public void handleRecordJournal() throws IOException {
        simpleRecordHandler.handleRecord("thread-1", 123, "journal", ("mytestjournal 6F3E7018B987ED1260B6B843364261BA " + new UUID(0, 0).toString()).getBytes());
        File blob = new File(blobDir, "journal-mytestjournal");
        assertTrue(blob.exists());
        assertTrue(blob.isFile());
        String actual = IOUtils.readString(new FileInputStream(blob));
        assertEquals("6F3E7018B987ED1260B6B843364261BA", actual);
    }
}