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

import static org.apache.jackrabbit.oak.store.zeromq.ZeroMQNodeStore.clusterInstanceForUuid;
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

    private static ZeroMQNodeStore _dummyStore = null;

    static ZeroMQNodeStore dummyStore() {

        System.setProperty("clusterInstances", "2");

        if (_dummyStore == null) {
            _dummyStore = new ZeroMQNodeStore() {

                @Override
                public @NotNull NodeState getRoot() {
                    return null;
                }

                @Override
                public @NotNull NodeState merge(@NotNull NodeBuilder builder, @NotNull CommitHook commitHook, @NotNull CommitInfo info) throws CommitFailedException {
                    return null;
                }

                @Override
                public @NotNull NodeState rebase(@NotNull NodeBuilder builder) {
                    return null;
                }

                @Override
                public NodeState reset(@NotNull NodeBuilder builder) {
                    return null;
                }

                @Override
                public @NotNull Blob createBlob(InputStream inputStream) throws IOException {
                    return null;
                }

                @Override
                public @Nullable Blob getBlob(@NotNull String reference) {
                    return null;
                }

                @Override
                public @NotNull String checkpoint(long lifetime, @NotNull Map<String, String> properties) {
                    return null;
                }

                @Override
                public @NotNull String checkpoint(long lifetime) {
                    return null;
                }

                @Override
                public @NotNull Map<String, String> checkpointInfo(@NotNull String checkpoint) {
                    return null;
                }

                @Override
                public @NotNull Iterable<String> checkpoints() {
                    return null;
                }

                @Override
                public @Nullable NodeState retrieve(@NotNull String checkpoint) {
                    return null;
                }

                @Override
                public boolean release(@NotNull String checkpoint) {
                    return false;
                }
            };
        }
        return _dummyStore;
    }

    String getSerialised(String sUuid) {
        final UUID uuid = UUID.fromString(sUuid);
        final StringBuilder sb = new StringBuilder();
        if (uuid.equals(UUIDS[0])) {
            sb
                    .append("begin ZeroMQNodeState ").append(uuid.toString()).append('\n')
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
                    .append("begin ZeroMQNodeState ").append(uuid.toString()).append('\n')
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
            ret = ZeroMQNodeState.deSerialise(dummyStore(), serialised, this::staticReader, this::storageWriter);
        } catch (ZeroMQNodeState.ParseFailure parseFailure) {
        }

        return ret;
    }

    private ZeroMQNodeState storageReader(String s) {
        final String ser = storage.get(s);
        try {
            return ZeroMQNodeState.deSerialise(dummyStore(), ser, this::storageReader, this::storageWriter);
        } catch (ZeroMQNodeState.ParseFailure parseFailure) {
            throw new IllegalStateException(parseFailure);
        }
    }
    private void storageWriter(ZeroMQNodeState.SerialisedZeroMQNodeState ns) {
        storage.put(ns.getUuid(), ns.getserialisedNodeState());
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
        ns.serialise(sb::append);
        assertEquals(getSerialised(UUIDS[0].toString()), sb.toString());

        sb = new StringBuilder();
        ((ZeroMQNodeState) ns.getChildNode("cOne")).serialise(sb::append);
        assertEquals(getSerialised(UUIDS[1].toString()), sb.toString());

        sb = new StringBuilder();
        ((ZeroMQNodeState) ns.getChildNode("cTwo")).serialise(sb::append);
        assertEquals(getSerialised(UUIDS[2].toString()), sb.toString());
    }

    // @Test
    // This test fails without a real node store
    public void diff() throws IOException {
        storage.clear();
        final ZeroMQNodeState ns = (ZeroMQNodeState) ZeroMQEmptyNodeState.EMPTY_NODE(dummyStore(), this::staticReader, this::storageWriter);
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

    @Test
    public void emptyArray() throws ZeroMQNodeState.ParseFailure {
        storage.clear();

        final ZeroMQNodeState ns = (ZeroMQNodeState) ZeroMQEmptyNodeState.EMPTY_NODE(dummyStore(), this::staticReader, this::storageWriter);
        final NodeBuilder builder = ns.builder();
        builder.setProperty("bla", new ArrayList()  , Type.STRINGS);
        final NodeState ns2 = builder.getNodeState();
        final PropertyState ps = ns2.getProperty("bla");
        assertTrue(ps.isArray());
        assertTrue(ps.count() == 0);
        assertTrue(ps.size() == 0);
        final List<ZeroMQNodeState.SerialisedZeroMQNodeState> ser = new ArrayList<>();
        ((ZeroMQNodeState) ns2).serialise(ser::add);
        final String s = ser.get(0).getserialisedNodeState();
        assertTrue(s.contains("[]"));
        final NodeState ns3 = ZeroMQNodeState.deSerialise(dummyStore(), s, this::staticReader, this::storageWriter);
        assertTrue(ns3.getProperty("bla").count() == 0);
    }

    @Test
    public void testClusterInstanceForSegmentId() {
        StringBuilder sb = new StringBuilder();
        for (UUID uuid : UUIDS) {
            sb.append(uuid.toString());
            sb.append(": ");
            sb.append(clusterInstanceForUuid(2, uuid.toString()));
            sb.append("\n");
        }
        throw(new Error(sb.toString()));
    }
}