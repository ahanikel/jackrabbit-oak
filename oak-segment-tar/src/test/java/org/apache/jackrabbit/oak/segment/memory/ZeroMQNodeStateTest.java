package org.apache.jackrabbit.oak.segment.memory;

import org.apache.jackrabbit.oak.api.Type;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class ZeroMQNodeStateTest {

    private static final int NUM_UUIDS = 3;
    private static final UUID[] UUIDS = new UUID[NUM_UUIDS];

    private ZeroMQNodeState ns;

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
                    .append("pOne <STRING> = Hello world\n")
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

    @Before
    public void setUp() throws Exception {
        ns = ZeroMQNodeState.newZeroMQNodeState(UUIDS[0].toString(), this::reader);
    }

    @After
    public void tearDown() throws Exception {
        ns = null;
    }

    @Test
    public void parse() {
        final ZeroMQNodeState cOne = (ZeroMQNodeState) ns.getChildNode("cOne");
        final ZeroMQNodeState cTwo = (ZeroMQNodeState) ns.getChildNode("cTwo");
        assertNotNull(cOne);
        assertNotNull(cTwo);

        assertEquals(Type.STRING, ns.getProperty("pOne").getType());
        assertEquals("Hello world", ns.getProperty("pOne").getValue(Type.STRING));

        assertEquals(Type.STRING, cOne.getProperty("pOne").getType());
        assertEquals("Hello world", ns.getProperty("pOne").getValue(Type.STRING));

        assertEquals(Type.STRING, cTwo.getProperty("pOne").getType());
        assertEquals("Hello world", ns.getProperty("pOne").getValue(Type.STRING));
    }

    @Test
    public void serialise() {
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
}