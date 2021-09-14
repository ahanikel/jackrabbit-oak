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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.MalformedURLException;

import static org.apache.jackrabbit.oak.store.zeromq.ZeroMQNodeStoreBuilder.PARAM_BACKENDREADER_URL;
import static org.apache.jackrabbit.oak.store.zeromq.ZeroMQNodeStoreBuilder.PARAM_INITJOURNAL;
import static org.apache.jackrabbit.oak.store.zeromq.ZeroMQNodeStoreBuilder.PARAM_REMOTEREADS;
import static org.apache.jackrabbit.oak.store.zeromq.ZeroMQNodeStoreBuilder.PARAM_WRITEBACKJOURNAL;
import static org.apache.jackrabbit.oak.store.zeromq.ZeroMQNodeStoreBuilder.PARAM_WRITEBACKNODES;
import static org.junit.Assert.*;

public class ZeroMQNodeStoreBuilderTest {

    private ZeroMQNodeStoreBuilder builder;

    @Before
    public void setUp() {
        builder = new ZeroMQNodeStoreBuilder();
    }

    @After
    public void tearDown() {
        builder = null;
    }

    @Test
    public void initFromURI() throws MalformedURLException {
        assertEquals("journalId", "golden", builder.getJournalId());
        assertTrue(PARAM_REMOTEREADS, builder.isRemoteReads());
        assertFalse(PARAM_WRITEBACKNODES, builder.isWriteBackNodes());
        assertEquals(PARAM_BACKENDREADER_URL, "tcp://localhost:8000", builder.getBackendReaderURL());
        assertNull(PARAM_INITJOURNAL, builder.getInitJournal());
        assertFalse(PARAM_WRITEBACKJOURNAL, builder.isWriteBackJournal());

        final StringBuilder sb = new StringBuilder("zeromq://someJournalId?");
        sb
                .append(PARAM_REMOTEREADS).append("=false")
                .append('&')
                .append(PARAM_WRITEBACKNODES).append("=true")
                .append('&')
                .append(PARAM_BACKENDREADER_URL).append("=tcp%3a%2f%2fbackend%3a9999")
                .append('&')
                .append(PARAM_INITJOURNAL).append("=12345-0")
                .append('&')
                .append(PARAM_WRITEBACKJOURNAL).append("=true");
        builder.initFromURIString(sb.toString());

        assertEquals("journalId", "someJournalId", builder.getJournalId());
        assertFalse(PARAM_REMOTEREADS, builder.isRemoteReads());
        assertTrue(PARAM_WRITEBACKNODES, builder.isWriteBackNodes());
        assertEquals(PARAM_BACKENDREADER_URL, "tcp://backend:9999", builder.getBackendReaderURL());
        assertEquals(PARAM_INITJOURNAL, "12345-0", builder.getInitJournal());
        assertTrue(PARAM_WRITEBACKJOURNAL, builder.isWriteBackJournal());
    }
}