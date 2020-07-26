package org.apache.jackrabbit.oak.store.zeromq;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.blob.AbstractBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class ZeroMQNodeStoreTest {

    final ZeroMQFixture fixture = new ZeroMQFixture();
    NodeStore ns;

    @Before
    public void setUp() {
        ns = fixture.createNodeStore();
    }

    @After
    public void tearDown() {
        fixture.dispose(ns);
    }

    @Test
    public void testSmallBlob() throws CommitFailedException, IOException {
        final byte[] bytes = new byte[] { 9, 8, 7, 0, -128, 3 };
        final Blob blob = ns.createBlob(new ByteArrayInputStream(bytes));
        final NodeBuilder root = ns.getRoot().builder();
        final NodeBuilder one = root.child("cOne");
        one.setProperty("jcr:data", blob);
        NodeState o = one.getNodeState();
        ns.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeState root2 = ns.getRoot();
        final PropertyState p = root2.getChildNode("cOne").getProperty("jcr:data");
        final InputStream is = p.getValue(Type.BINARY).getNewStream();
        int i;
        int b;
        for (i = 0, b = is.read(); b >= 0; ++i, b = is.read()) {
            assertEquals(bytes[i], (byte) b);
        }
        assertEquals(bytes.length, i);
    }

    @Test
    public void testLargerBlob() throws CommitFailedException, IOException {
        byte[] bytes = new byte[2 * 4096];
        new Random(System.currentTimeMillis()).nextBytes(bytes);
        final Blob blob = ns.createBlob(new ByteArrayInputStream(bytes));
        final NodeBuilder root = ns.getRoot().builder();
        final NodeBuilder one = root.child("cOne");
        one.setProperty("jcr:data", blob);
        NodeState o = one.getNodeState();
        ns.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeState root2 = ns.getRoot();
        final PropertyState p = root2.getChildNode("cOne").getProperty("jcr:data");
        final InputStream is = p.getValue(Type.BINARY).getNewStream();
        int i;
        int b;
        for (i = 0, b = is.read(); b >= 0; ++i, b = is.read()) {
            assertEquals(bytes[i], (byte) b);
        }
        assertEquals(bytes.length, i);
    }
}
