package org.apache.jackrabbit.oak.store.zeromq;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.commit.AnnotatingConflictHandler;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHook;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.ResetCommitAttributeHook;
import org.apache.jackrabbit.oak.spi.commit.SimpleCommitContext;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.store.zeromq.log.LogBackendStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ZeroMQPubSubTest {
    private ZeroMQBackendStore logBackendStore;

    final char[] ref = "Hello world".toCharArray();

    interface Condition {
        boolean conditionFulfilled();
    }

    private static final Logger log = LoggerFactory.getLogger(ZeroMQPubSubTest.class.getName());
    private static final ZeroMQFixture fixture = new ZeroMQFixture();

    private ZeroMQNodeStore store1;
    private ZeroMQNodeStore store2;

    @Before
    public void setup() throws Exception {
        final File logFile = File.createTempFile("ZeroMQFixture", ".log");
        logBackendStore = LogBackendStore.builder()
            .withLogFile(logFile.getAbsolutePath())
            .withReaderUrl("ipc:///tmp/fixtureBackendReader")
            .withWriterUrl("ipc:///tmp/fixtureBackendWriter")
            .withNumThreads(4)
            .build();
        store1 = ZeroMQNodeStore.builder()
            .setJournalId("test")
            .setBackendReaderURL("ipc:///tmp/fixtureBackendReader")
            .setBackendWriterURL("ipc:///tmp/fixtureBackendWriter")
            .setBlobCacheDir(Files.createTempDir().getAbsolutePath())
            .setWriteBackNodes(true)
            .setWriteBackJournal(true)
            .build();
        store1.reset();
        Thread.sleep(1000); // TODO: Wait until a commit is confirmed

        NodeState root = store1.getRoot();
        NodeBuilder builder = root.builder();
        builder.child("content");
        store1.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store2 = ZeroMQNodeStore.builder()
            .setJournalId("test")
            .setBackendReaderURL("ipc:///tmp/fixtureBackendReader")
            .setBackendWriterURL("ipc:///tmp/fixtureBackendWriter")
            .setBlobCacheDir(Files.createTempDir().getAbsolutePath())
            .setWriteBackNodes(true)
            .setWriteBackJournal(true)
            .build();
    }

    @After
    public void tearDown() {
        log.info("tearDown: start");
        if (store2 != null) {
            store2.close();
            store2 = null;
        }
        if (store1 != null) {
            store1.close();
            store1 = null;
        }
        if (logBackendStore != null) {
            logBackendStore.close();
            logBackendStore = null;
        }
        log.info("tearDown: done");
    }

    @Test
    public void sendMessage() {
        sendAMessage(store1, "prop1", "value1");
    }

    private static CommitInfo createCommitInfo() {
        Map<String, Object> info = ImmutableMap.<String, Object>of(CommitContext.NAME, new SimpleCommitContext());
        return new CommitInfo(CommitInfo.OAK_UNKNOWN, CommitInfo.OAK_UNKNOWN, info);
    }

    private void sendAMessage(NodeStore ns, String key, String value) {
        NodeBuilder rootBuilder = ns.getRoot().builder();
        rootBuilder.child("content").child("node1").setProperty(key, value);
        try {
            ns.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void sendAMessageWithoutCatchingAnything(NodeStore ns, String key, String value) throws CommitFailedException {
        NodeBuilder rootBuilder = ns.getRoot().builder();
        rootBuilder.child("content").child("node1").setProperty(key, value);
        CompositeHook hooks = new CompositeHook(
            ResetCommitAttributeHook.INSTANCE,
            new ConflictHook(new AnnotatingConflictHandler()),
            new EditorHook(CompositeEditorProvider.compose(singletonList(new ConflictValidatorProvider())))
        );
        ns.merge(rootBuilder, hooks, createCommitInfo());
    }

    @Test
    public void sendAndReceiveMessage() throws InterruptedException {
        sendAMessage(store1, "prop1", "value1");

        assertTrue(waitMaxUntil(5000000000L, new Condition() {

            @Override
            public boolean conditionFulfilled() {
                assertTrue(store2.getRoot().hasChildNode("content"));
                assertTrue(store2.getRoot().getChildNode("content").hasChildNode("node1"));
                return true;
            }

        }));
    }

    private boolean waitMaxUntil(long maxNanos, Condition c) throws InterruptedException {
        final long start = System.nanoTime();
        while (System.nanoTime() < start + maxNanos) {
            try {
                if (c.conditionFulfilled()) {
                    return true;
                }
            } catch (AssertionError e) {
            }
            Thread.sleep(5);
        }
        return false;
    }

    @Test
    public void multipleWritersTest() throws Exception {
        ZeroMQNodeStore store3 = ZeroMQNodeStore.builder()
            .setJournalId("test")
            .setBackendReaderURL("ipc:///tmp/fixtureBackendReader")
            .setBackendWriterURL("ipc:///tmp/fixtureBackendWriter")
            .setBlobCacheDir(Files.createTempDir().getAbsolutePath())
            .setWriteBackNodes(true)
            .setWriteBackJournal(true)
            .build();
        boolean b1 = store1.getRoot().hasChildNode("content");
        boolean b2 = store2.getRoot().hasChildNode("content");
        boolean b3 = store3.getRoot().hasChildNode("content");
        try {
            assertTrue(waitMaxUntil(2000000000L, new Condition() {

                @Override
                public boolean conditionFulfilled() {
                    assertTrue(store1.getRoot().hasChildNode("content"));
                    assertTrue(store2.getRoot().hasChildNode("content"));
                    return true;
                }

            }));

            assertTrue(waitMaxUntil(5000000000L, new Condition() {

                @Override
                public boolean conditionFulfilled() {
                    assertTrue(store3.getRoot().hasChildNode("content"));
                    return true;
                }

            }));

            sendAMessage(store3, "prop3", "value3");

            assertTrue(waitMaxUntil(2000000000L, new Condition() {

                @Override
                public boolean conditionFulfilled() {
                    assertTrue(store1.getRoot().hasChildNode("content"));
                    assertTrue(store1.getRoot().getChildNode("content").hasChildNode("node1"));
                    assertTrue(store1.getRoot().getChildNode("content").getChildNode("node1").hasProperty("prop3"));

                    assertTrue(store2.getRoot().hasChildNode("content"));
                    assertTrue(store2.getRoot().getChildNode("content").hasChildNode("node1"));
                    assertTrue(store2.getRoot().getChildNode("content").getChildNode("node1").hasProperty("prop3"));

                    return true;
                }

            }));

        } finally {
            store3.close();
        }
    }

    @Test
    @Ignore
    public void twoConcurrentIdenticalWrites() throws Exception {
        //store1.blockBackgroundReader(true);
        //store2.blockBackgroundReader(true);
        new Thread(() -> {
            sendAMessage(store1, "prop1", "value1");
        }).start();
        new Thread(() -> {
            sendAMessage(store2, "prop1", "value1");
        }).start();
        //assertTrue(store1.waitForBlockedBackgroundReader(5000));
        //assertTrue(store2.waitForBlockedBackgroundReader(5000));
        //store1.blockBackgroundReader(false);
        Thread.sleep(2000); // TODO: remove me!
        //store2.blockBackgroundReader(false);

        System.out.println("break");
//        now it conflicts
    }

    @Test
    @Ignore
    public void twoConcurrentDifferentWrites() throws Exception {
        final Semaphore store1Done = new Semaphore(0);
        final AtomicReference<Boolean> store2Result = new AtomicReference<>();
        //store1.blockBackgroundReader(true);
        //store2.blockBackgroundReader(true);
        new Thread(new Runnable() {
            public void run() {
                sendAMessage(store1, "prop1", "value1");
                store1Done.release();
            }
        }).start();
        new Thread(new Runnable() {
            public void run() {
                try {
                    sendAMessageWithoutCatchingAnything(store2, "prop1", "value2");
                    // not good, mate!
                    store2Result.set(false);
                } catch (CommitFailedException e) {
                    // this is expected
                    store2Result.set(true);
                }
            }
        }).start();
        //assertTrue(store1.waitForBlockedBackgroundReader(5000));
        //assertTrue(store2.waitForBlockedBackgroundReader(5000));
        //store1.blockBackgroundReader(false);
        assertTrue(store1Done.tryAcquire(5, TimeUnit.SECONDS));
        //store2.blockBackgroundReader(false);

        final long timeout = System.currentTimeMillis() + 60 * 1000;
        while (store2Result.get() == null && System.currentTimeMillis() < timeout) {
            Thread.sleep(100);
        }
        assertTrue(store2Result.get());
    }
}
