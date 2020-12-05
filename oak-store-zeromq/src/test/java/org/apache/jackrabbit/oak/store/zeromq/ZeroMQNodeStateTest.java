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

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.StringBasedBlob;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.*;

import static org.junit.Assert.*;

public class ZeroMQNodeStateTest {

    private static final int NUM_UUIDS = 6;
    private static final UUID[] UUIDS = new UUID[NUM_UUIDS];

    private final Map<String, String> storage = new HashMap<>();

    static {
        for (int i = 0; i < NUM_UUIDS; ++i) {
            UUIDS[i] = UUID.nameUUIDFromBytes(new byte[]{(byte) i, 0, 0, 0, 0, 0, 0, 0});
        }
    }


    String getSerialised(String sUuid) {
        final UUID uuid = UUID.fromString(sUuid);
        final StringBuilder sb = new StringBuilder();
        if (uuid.equals(UUIDS[0])) {
            sb
                    .append("begin ZeroMQNodeState\n")
                    .append("begin children\n")
                    .append("cOne\t").append(UUIDS[1]).append('\n')
                    .append("cTwo\t").append(UUIDS[2]).append('\n')
                    .append("end children\n")
                    .append("begin properties\n")
                    .append("pString <STRING> = Hello+world\n")
                    .append("pDate <DATE> = 2019-06-03T14:29:30%2B0200\n")
                    .append("pName <NAME> = Hello\n")
                    .append("pPath <PATH> = /Hello/World\n")
                    .append("pLong <LONG> = 1234567\n")
                    .append("pDouble <DOUBLE> = 1234567.89123\n")
                    .append("pBoolean <BOOLEAN> = true\n")
                    .append("pDecimal <DECIMAL> = 1234567\n")
                    .append("end properties\n")
                    .append("end ZeroMQNodeState\n");
        } else {
            sb
                    .append("begin ZeroMQNodeState\n")
                    .append("begin children\n")
                    .append("end children\n")
                    .append("begin properties\n")
                    .append("pOne <STRING> = Hello+world\n")
                    .append("end properties\n")
                    .append("end ZeroMQNodeState\n");
        }
        return sb.toString();
    }

    ZeroMQNodeState staticReader(String sUuid) {

        String serialised = getSerialised(sUuid);
        ZeroMQNodeState ret = null;

        try {
            ret = ZeroMQNodeState.deSerialise(null, serialised, this::staticReader, this::storageWriter);
        } catch (ZeroMQNodeState.ParseFailure parseFailure) {
        }

        return ret;
    }

    private ZeroMQNodeState storageReader(String s) {
        final String ser = storage.get(s);
        try {
            return ZeroMQNodeState.deSerialise(null, ser, this::storageReader, this::storageWriter);
        } catch (ZeroMQNodeState.ParseFailure parseFailure) {
            throw new IllegalStateException(parseFailure);
        }
    }
    private void storageWriter(ZeroMQNodeState ns) {
        storage.put(ns.getUuid(), ns.getSerialised());
    }

    @Test
    public void parse() {
        storage.clear();
        final ZeroMQNodeState ns = staticReader(UUIDS[0].toString());
        final ZeroMQNodeState cOne = (ZeroMQNodeState) ns.getChildNode("cOne");
        final ZeroMQNodeState cTwo = (ZeroMQNodeState) ns.getChildNode("cTwo");

        assertNotNull(cOne);
        assertNotNull(cTwo);

        assertEquals(Type.STRING, ns.getProperty("pString").getType());
        assertEquals("Hello world", ns.getProperty("pString").getValue(Type.STRING));

        assertEquals(Type.DATE, ns.getProperty("pDate").getType());
        assertEquals("2019-06-03T14:29:30+0200", ns.getProperty("pDate").getValue(Type.DATE));

        assertEquals(Type.NAME, ns.getProperty("pName").getType());
        assertEquals("Hello", ns.getProperty("pName").getValue(Type.NAME));

        assertEquals(Type.PATH, ns.getProperty("pPath").getType());
        assertEquals("/Hello/World", ns.getProperty("pPath").getValue(Type.PATH));

        assertEquals(Type.LONG, ns.getProperty("pLong").getType());
        assertEquals(new Long(1234567L), ns.getProperty("pLong").getValue(Type.LONG));

        assertEquals(Type.DOUBLE, ns.getProperty("pDouble").getType());
        assertEquals(new Double(1234567.89123d), ns.getProperty("pDouble").getValue(Type.DOUBLE));

        assertEquals(Type.BOOLEAN, ns.getProperty("pBoolean").getType());
        assertEquals(true, ns.getProperty("pBoolean").getValue(Type.BOOLEAN));

        assertEquals(Type.DECIMAL, ns.getProperty("pDecimal").getType());
        assertEquals(new BigDecimal(1234567), ns.getProperty("pDecimal").getValue(Type.DECIMAL));

        assertEquals(Type.STRING, cOne.getProperty("pOne").getType());
        assertEquals("Hello world", cOne.getProperty("pOne").getValue(Type.STRING));

        assertEquals(Type.STRING, cTwo.getProperty("pOne").getType());
        assertEquals("Hello world", cTwo.getProperty("pOne").getValue(Type.STRING));
    }

    // @Test
    // This test always fails because the order of children and properties is not defined
    // It can still be useful for manual testing
    public void serialise() {
        storage.clear();
        final ZeroMQNodeState ns = staticReader(UUIDS[0].toString());
        StringBuilder sb = new StringBuilder();
        sb.append(ns.getSerialised());
        assertEquals(getSerialised(UUIDS[0].toString()), sb.toString());

        sb = new StringBuilder();
        sb.append(((ZeroMQNodeState) ns.getChildNode("cOne")).getSerialised());
        assertEquals(getSerialised(UUIDS[1].toString()), sb.toString());

        sb = new StringBuilder();
        sb.append(((ZeroMQNodeState) ns.getChildNode("cTwo")).getSerialised());
        assertEquals(getSerialised(UUIDS[2].toString()), sb.toString());
    }

    // @Test
    // This test fails without a real node store
    public void diff() throws IOException {
        storage.clear();
        final ZeroMQNodeState ns = (ZeroMQNodeState) ZeroMQEmptyNodeState.EMPTY_NODE(null, this::staticReader, this::storageWriter);
        final NodeBuilder builder = ns.builder();
        builder.child("first")
                .setProperty("1p", "blurb", Type.STRING)
                .setProperty("2p", 3L, Type.LONG)
                .setProperty("3p", 5.0, Type.DOUBLE)
                .setProperty("4p", true, Type.BOOLEAN)
                .setProperty("5p", new BigDecimal(7), Type.DECIMAL)
                .setProperty("6p", new StringBasedBlob("Hello world"), Type.BINARY)
        ;
        builder.child("[nt:base]")
                .setProperty("1p", "blah", Type.STRING)
                .setProperty("2p", 4L, Type.LONG)
                .setProperty("3p", 6.0, Type.DOUBLE)
                .setProperty("4p", false, Type.BOOLEAN)
                .setProperty("5p", new BigDecimal(8), Type.DECIMAL)
                .setProperty("6p", new StringBasedBlob("Hello region"), Type.BINARY)
        ;
        builder.child("[empty:node]");

        final NodeState newNs = builder.getNodeState();
        final String uuid = ((ZeroMQNodeState) newNs).getUuid();
        final NodeState nsRead = storageReader(uuid);

        assertTrue(nsRead.hasChildNode("first"));
        assertTrue(nsRead.getChildNode("first").hasProperty("1p"));
        assertTrue(nsRead.getChildNode("first").getProperty("1p").getValue(Type.STRING).equals("blurb"));
        assertTrue(nsRead.getChildNode("first").hasProperty("2p"));
        assertTrue(nsRead.getChildNode("first").getProperty("2p").getValue(Type.LONG).equals(3L));
        assertTrue(nsRead.getChildNode("first").hasProperty("3p"));
        assertTrue(nsRead.getChildNode("first").getProperty("3p").getValue(Type.DOUBLE).equals(5.0));
        assertTrue(nsRead.getChildNode("first").hasProperty("4p"));
        assertTrue(nsRead.getChildNode("first").getProperty("4p").getValue(Type.BOOLEAN).equals(true));
        assertTrue(nsRead.getChildNode("first").hasProperty("5p"));
        assertTrue(nsRead.getChildNode("first").getProperty("5p").getValue(Type.DECIMAL).equals(new BigDecimal(7)));
        assertTrue(nsRead.getChildNode("first").hasProperty("6p"));
        ZeroMQPropertyState ps = (ZeroMQPropertyState) nsRead.getChildNode("first").getProperty("6p");
        Blob v = ps.getValue(Type.BINARY);
        final char[] ref = "Hello world".toCharArray();
        InputStream is = v.getNewStream();
        for (int i = 0; i < ref.length; ++i) {
            assertEquals((int) ref[i], is.read());
        }
        assertEquals(-1, is.read());

        assertTrue(nsRead.hasChildNode("[nt:base]"));
        assertTrue(nsRead.getChildNode("[nt:base]").hasProperty("1p"));
        assertTrue(nsRead.getChildNode("[nt:base]").getProperty("1p").getValue(Type.STRING).equals("blah"));
        assertTrue(nsRead.getChildNode("[nt:base]").hasProperty("2p"));
        assertTrue(nsRead.getChildNode("[nt:base]").getProperty("2p").getValue(Type.LONG).equals(4L));
        assertTrue(nsRead.getChildNode("[nt:base]").hasProperty("3p"));
        assertTrue(nsRead.getChildNode("[nt:base]").getProperty("3p").getValue(Type.DOUBLE).equals(6.0));
        assertTrue(nsRead.getChildNode("[nt:base]").hasProperty("4p"));
        assertTrue(nsRead.getChildNode("[nt:base]").getProperty("4p").getValue(Type.BOOLEAN).equals(false));
        assertTrue(nsRead.getChildNode("[nt:base]").hasProperty("5p"));
        assertTrue(nsRead.getChildNode("[nt:base]").getProperty("5p").getValue(Type.DECIMAL).equals(new BigDecimal(8)));
        assertTrue(nsRead.getChildNode("[nt:base]").hasProperty("6p"));
        ZeroMQPropertyState ps2 = (ZeroMQPropertyState) nsRead.getChildNode("[nt:base]").getProperty("6p");
        Blob v2 = ps2.getValue(Type.BINARY);
        final char[] ref2 = "Hello region".toCharArray();
        InputStream is2 = v2.getNewStream();
        for (int i = 0; i < ref2.length; ++i) {
            assertEquals((int) ref2[i], is2.read());
        }
        assertEquals(-1, is2.read());

        assertTrue(nsRead.getChildNode("[empty:node]").exists());
    }

    // TODO: needs to be rewritten
    /*
    @Test
    public void stringToBlob() throws IOException {
        final Blob blob = ZeroMQBlob.newInstance("48656C6C6F20776F726C64");
        final InputStream is = blob.getNewStream();
        final char[] ref = "Hello world".toCharArray();
        for (int i = 0; i < ref.length; ++i) {
            assertEquals((int) ref[i], is.read());
        }
        assertEquals(-1, is.read());
    }
    */

    // @Test
    // This test fails without a real node store
    public void emptyArray() throws ZeroMQNodeState.ParseFailure {
        storage.clear();

        final ZeroMQNodeState ns = (ZeroMQNodeState) ZeroMQEmptyNodeState.EMPTY_NODE(null, this::staticReader, this::storageWriter);
        final NodeBuilder builder = ns.builder();
        builder.setProperty("bla", new ArrayList<String>()  , Type.STRINGS);
        final NodeState ns2 = builder.getNodeState();
        final PropertyState ps = ns2.getProperty("bla");
        assertTrue(ps.isArray());
        assertTrue(ps.count() == 0);
        try {
            ps.size();
            assertTrue("PropertyState.size() should throw IllegalStateException if isArray()", false);
        } catch (IllegalStateException e) {
        }
        final String s = ((ZeroMQNodeState) ns2).getSerialised();
        assertTrue(s.contains("[]"));
        final NodeState ns3 = ZeroMQNodeState.deSerialise(null, s, this::staticReader, this::storageWriter);
        assertTrue(ns3.getProperty("bla").count() == 0);
    }

    /*
    @Test
    public void testClusterInstanceForSegmentId() {
        final int[] expected = {0, 0, 0, 0, 1, 0};
        for (int i = 0; i < UUIDS.length; ++i) {
            assertEquals(expected[i], clusterInstanceForUuid(2, UUIDS[i].toString()));
        }
    }
    */
}
