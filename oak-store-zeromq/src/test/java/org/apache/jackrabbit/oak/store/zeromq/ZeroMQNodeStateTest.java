package org.apache.jackrabbit.oak.store.zeromq;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class ZeroMQNodeStateTest {

    private static final int NUM_UUIDS = 3;
    private static final UUID[] UUIDS = new UUID[NUM_UUIDS];

    private final Map<String, String> storage = new HashMap<>();

    static {
        for (int i = 0; i < NUM_UUIDS; ++i) {
            UUIDS[i] = UUID.nameUUIDFromBytes(new byte[] {(byte) i,0,0,0,0,0,0,0});
        }
    }

    private String reader(String sUuid) {
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
                    .append("pString <STRING> = Hello world\n")
                    .append("pDate <DATE> = 2019-06-03T14:29:30+0200\n")
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
                    .append("pOne <STRING> = Hello world\n")
                    .append("end properties\n")
                    .append("end ZeroMQNodeState\n");
        }
        return sb.toString();
    }

    private void writer(String s) {
        final Pattern uuidPattern = Pattern.compile("begin ZeroMQNodeState ([^\\n]+).*", Pattern.DOTALL);
        final Matcher m = uuidPattern.matcher(s);
        if (m.matches()) {
            final String uuid = m.group(1);
            storage.put(uuid, s);
        }
    }

    @Test
    public void parse() {
        storage.clear();
        final ZeroMQNodeState ns = ZeroMQNodeState.newZeroMQNodeState(UUIDS[0].toString(), this::reader, this::writer);
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
        assertEquals(1234567L, ns.getProperty("pLong").getValue(Type.PATH));

        assertEquals(Type.DOUBLE, ns.getProperty("pDouble").getType());
        assertEquals(1234567.89123d, ns.getProperty("pDouble").getValue(Type.PATH));

        assertEquals(Type.BOOLEAN, ns.getProperty("pBoolean").getType());
        assertEquals(true, ns.getProperty("pBoolean").getValue(Type.PATH));

        assertEquals(Type.DECIMAL, ns.getProperty("pDecimal").getType());
        assertEquals(new BigDecimal(1234567), ns.getProperty("pDecimal").getValue(Type.DECIMAL));

        assertEquals(Type.STRING, cOne.getProperty("pOne").getType());
        assertEquals("Hello world", cOne.getProperty("pOne").getValue(Type.STRING));

        assertEquals(Type.STRING, cTwo.getProperty("pOne").getType());
        assertEquals("Hello world", cTwo.getProperty("pOne").getValue(Type.STRING));
    }

    @Test
    public void serialise() {
        storage.clear();
        final ZeroMQNodeState ns = ZeroMQNodeState.newZeroMQNodeState(UUIDS[0].toString(), this::reader, this::writer);
        StringBuilder sb = new StringBuilder();
        ns.serialise(sb::append);
        assertEquals(reader(UUIDS[0].toString()), sb.toString());

        sb = new StringBuilder();
        ((ZeroMQNodeState) ns.getChildNode("cOne")).serialise(sb::append);
        assertEquals(reader(UUIDS[1].toString()), sb.toString());

        sb = new StringBuilder();
        ((ZeroMQNodeState) ns.getChildNode("cTwo")).serialise(sb::append);
        assertEquals(reader(UUIDS[2].toString()), sb.toString());
    }

    @Test
    public void diff() {
        storage.clear();
        final ZeroMQNodeState ns = (ZeroMQNodeState) ZeroMQEmptyNodeState.EMPTY_NODE(this::reader, this::writer);
        final NodeBuilder builder = ns.builder();
        builder.child("first").setProperty("fp", "blurb");
        final NodeState newNs = builder.getNodeState();
        final String uuid = ((ZeroMQNodeState) newNs).getUuid();
        final String ser = storage.get(uuid);
        final NodeState nsRead = ZeroMQNodeState.newZeroMQNodeState(uuid, storage::get, s -> {});
        assertTrue(nsRead.hasChildNode("first"));
        assertTrue(nsRead.getChildNode("first").hasProperty("fp"));
        assertTrue(nsRead.getChildNode("first").getProperty("fp").getValue(Type.STRING).equals("blurb"));
    }
}