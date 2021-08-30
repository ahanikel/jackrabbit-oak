package org.apache.jackrabbit.oak.store.zeromq;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ZeroMQBlobTest {
    private static final ZeroMQFixture fixture = new ZeroMQFixture();
    private static final Logger log = LoggerFactory.getLogger(ZeroMQBlobTest.class);
    private static final int blobSize = 1024*1024*100;
    private static final byte[] bytes = new byte[blobSize];
    private String refRef;
    private ZeroMQNodeStore store;

    @Before
    public void setup() throws ZeroMQNodeState.ParseFailure, NoSuchAlgorithmException {
        // temporary workaround
        File[] blobs = new File("/tmp/blobs").listFiles();
        if (blobs != null) {
            for (File f : blobs) {
                f.delete();
            }
        }

        store = (ZeroMQNodeStore) fixture.createNodeStore();

        // initialise blob array
        for (int i = 0; i < bytes.length; ++i) {
            bytes[i] = (byte) (i & 0xff);
        }

        // get blob array md5 checksum
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(bytes);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < digest.length; ++i) {
            sb.append(String.format("%02x", digest[i]));
        }
        refRef = sb.toString().toUpperCase();
    }

    @After
    public void tearDown() {
        fixture.dispose(store);
    }

    @Test
    public void transferBlob() throws IOException, CommitFailedException {
        NodeState rootState = store.getRoot();
        NodeBuilder root = rootState.builder();
        Blob blob = store.createBlob(new ByteArrayInputStream(bytes));
        String ref = blob.getReference();
        assertEquals("Reference must be the same after reading", refRef, ref);
        root.setProperty("theBlob", blob);
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // make sure the blob is actually sent from backend to frontend
        for (File f : fixture.getCacheDir().listFiles()) {
            assertTrue(f.delete());
        }

        // wait until the aggregator has processed things
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
        }

        store.reInit();

        NodeState newRoot = store.getRoot();
        Blob newBlob = newRoot.getProperty("theBlob").getValue(Type.BINARY);
        String newRef = newBlob.getReference();
        byte[] newBytes = new byte[blobSize];
        int nBytes = newBlob.getNewStream().read(newBytes);
        assertEquals("We must have read all the bytes", blobSize, nBytes);
        assertEquals("Reference must be the same after reading", refRef, newRef);
    }
}
