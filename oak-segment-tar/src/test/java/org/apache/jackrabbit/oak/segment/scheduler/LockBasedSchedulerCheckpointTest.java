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

package org.apache.jackrabbit.oak.segment.scheduler;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.segment.SegmentNodeStoreStats;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockBasedSchedulerCheckpointTest {

    private static final Logger log = LoggerFactory.getLogger(LockBasedSchedulerCheckpointTest.class);

    /**
     * OAK-3587 test simulates a timeout while trying to create a checkpoint,
     * then releases the lock and tries again
     */
    @Test
    public void testShortWait() throws Exception {
        MemoryStore ms = new MemoryStore();
        System.setProperty("oak.checkpoints.lockWaitTime", "1");
        StatisticsProvider statsProvider = StatisticsProvider.NOOP;
        SegmentNodeStoreStats stats = new SegmentNodeStoreStats(statsProvider);
        final LockBasedScheduler scheduler = LockBasedScheduler.builder(ms.getRevisions(), ms.getReader(), stats)
                .build();

        final Semaphore semaphore = new Semaphore(0);
        final AtomicBoolean blocking = new AtomicBoolean(true);

        final Callable<Boolean> block = new Callable<Boolean>() {

            @Override
            public Boolean call() {
                while (blocking.get()) {
                    if (semaphore.availablePermits() == 0) {
                        semaphore.release();
                    }
                }
                return true;
            }
        };

        Thread background = new Thread() {
            @Override
            public void run() {
                try {
                    Commit commit = createBlockingCommit(scheduler, "foo", "bar", block);
                    scheduler.schedule(commit);
                } catch (Exception e) {
                    //
                }
            }
        };

        background.start();
        semaphore.acquire();

        String cp0 = scheduler.checkpoint(10, Collections.<String, String> emptyMap());
        assertNull(retrieveCheckpoint(scheduler, cp0));

        blocking.set(false);
        String cp1 = scheduler.checkpoint(10, Collections.<String, String> emptyMap());
        assertNotNull(retrieveCheckpoint(scheduler, cp1));
    }

    /**
     * OAK-3587 test simulates a wait less than configured
     * {@code SegmentNodeStore#setCheckpointsLockWaitTime(int)} value so the
     * checkpoint call must return a valid value
     */
    @Test
    public void testLongWait() throws Exception {
        final int blockTime = 1;
        MemoryStore ms = new MemoryStore();
        System.setProperty("oak.checkpoints.lockWaitTime", "2");
        StatisticsProvider statsProvider = StatisticsProvider.NOOP;
        SegmentNodeStoreStats stats = new SegmentNodeStoreStats(statsProvider);
        final LockBasedScheduler scheduler = LockBasedScheduler.builder(ms.getRevisions(), ms.getReader(), stats)
                .build();

        final Semaphore semaphore = new Semaphore(0);

        final Callable<Boolean> block = new Callable<Boolean>() {

            @Override
            public Boolean call() {
                try {
                    semaphore.release();
                    SECONDS.sleep(blockTime);
                } catch (InterruptedException e) {
                    //
                }
                return true;
            }
        };

        Thread background = new Thread() {
            @Override
            public void run() {
                try {
                    Commit commit = createBlockingCommit(scheduler, "foo", "bar", block);
                    scheduler.schedule(commit);
                } catch (Exception e) {
                    //
                }
            }
        };

        background.start();
        semaphore.acquire();

        String cp0 = scheduler.checkpoint(10, Collections.<String, String> emptyMap());
        assertNotNull(retrieveCheckpoint(scheduler, cp0));
    }

    @Test
    public void testLongCommit() throws Exception {
        final int blockTime = 30;
        MemoryStore ms = new MemoryStore();
        System.setProperty("oak.checkpoints.lockWaitTime", "2");
        StatisticsProvider statsProvider = StatisticsProvider.NOOP;
        SegmentNodeStoreStats stats = new SegmentNodeStoreStats(statsProvider);
        final LockBasedScheduler scheduler = LockBasedScheduler.builder(ms.getRevisions(), ms.getReader(), stats)
                .build();

        final Semaphore semaphore = new Semaphore(0);

        final Callable<Boolean> block = new Callable<Boolean>() {

            @Override
            public Boolean call() {
                try {
                    SECONDS.sleep(blockTime);
                    semaphore.release();
                } catch (InterruptedException e) {
                    throw new IllegalStateException("block has been interrupted");
                }
                return true;
            }
        };

        Thread blockingCommitThread = new Thread() {
            @Override
            public void run() {
                try {
                    Commit commit = createBlockingCommit(scheduler, "blocking", "blocking", block);
                    scheduler.schedule(commit);
                    log.info("Blocking commit scheduled");
                } catch (Exception e) {
                    log.error(e.toString());
                }
            }
        };

        Thread followingCommitThread = new Thread() {
            @Override
            public void run() {
                try {
                    Commit commit = createBlockingCommit(scheduler, "following", "following", () -> { return true; });
                    scheduler.schedule(commit);
                    log.info("Following commit scheduled");
                } catch (Exception e) {
                    log.error(e.toString());
                }
            }
        };

        log.info("Background thread starting");
        blockingCommitThread.start();
        log.info("Background thread started, starting following commit thread");
        followingCommitThread.start();
        log.info("Following thread started, acquiring semaphore");
        semaphore.acquire();
        log.info("Semaphore acquired");

    }

    private NodeState retrieveCheckpoint(final Scheduler scheduler, final String checkpoint) {
        checkNotNull(checkpoint);
        NodeState cp = scheduler.getHeadNodeState().getChildNode("checkpoints").getChildNode(checkpoint)
                .getChildNode("root");
        if (cp.exists()) {
            return cp;
        }
        return null;
    }

    private NodeState getRoot(Scheduler scheduler) {
        return scheduler.getHeadNodeState().getChildNode("root");
    }

    private Commit createBlockingCommit(final Scheduler scheduler, final String property, String value,
            final Callable<Boolean> callable) {
        NodeBuilder a = getRoot(scheduler).builder();
        a.setProperty(property, value);
        Commit blockingCommit = new Commit(a, new CommitHook() {
            @Override
            @NotNull
            public NodeState processCommit(NodeState before, NodeState after, CommitInfo info) {
                try {
                    callable.call();
                    log.info(property + "callable call finished");
                } catch (Exception e) {
                    fail();
                }
                return after;
            }
        }, CommitInfo.EMPTY);

        return blockingCommit;
    }
}
