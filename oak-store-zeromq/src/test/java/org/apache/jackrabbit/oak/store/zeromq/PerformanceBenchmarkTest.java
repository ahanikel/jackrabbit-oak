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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Performance benchmark tests to identify and measure bottlenecks in oak-store-zeromq
 */
public class PerformanceBenchmarkTest {

    private SimpleNodeStore store;
    private ZeroMQFixture fixture;

    @Before
    public void setUp() throws IOException {
        fixture = new ZeroMQFixture();
        store = (SimpleNodeStore) fixture.createNodeStore();
    }

    @After
    public void tearDown() throws Exception {
        // Fixture doesn't have close method
    }

    /**
     * Benchmark 1: Measure cold read performance (cache miss scenario)
     * Tests the overhead of lazy loading with synchronization
     */
    @Test
    public void testColdReadPerformance() throws CommitFailedException {
        // Create a tree structure
        NodeBuilder root = store.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            NodeBuilder child = root.child("child" + i);
            for (int j = 0; j < 10; j++) {
                child.child("subchild" + j).setProperty("prop", "value" + j);
            }
        }
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Clear caches to simulate cold start
        store.emptyCaches();

        // Measure time to read entire tree
        long startTime = System.nanoTime();
        NodeState readRoot = store.getRoot();
        int nodeCount = 0;
        int propertyCount = 0;

        for (String childName : readRoot.getChildNodeNames()) {
            nodeCount++;
            NodeState child = readRoot.getChildNode(childName);
            propertyCount += child.getPropertyCount();

            for (String subchildName : child.getChildNodeNames()) {
                nodeCount++;
                NodeState subchild = child.getChildNode(subchildName);
                propertyCount += subchild.getPropertyCount();
            }
        }

        long duration = System.nanoTime() - startTime;
        double durationMs = duration / 1_000_000.0;

        System.out.println("=== Cold Read Performance ===");
        System.out.println("Nodes read: " + nodeCount);
        System.out.println("Properties read: " + propertyCount);
        System.out.println("Time: " + durationMs + " ms");
        System.out.println("Avg time per node: " + (durationMs / nodeCount) + " ms");
        System.out.println();

        assertEquals(110, nodeCount); // 10 + 100
    }

    /**
     * Benchmark 2: Measure warm read performance (cache hit scenario)
     */
    @Test
    public void testWarmReadPerformance() throws CommitFailedException {
        // Create a tree structure
        NodeBuilder root = store.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            NodeBuilder child = root.child("child" + i);
            for (int j = 0; j < 10; j++) {
                child.child("subchild" + j).setProperty("prop", "value" + j);
            }
        }
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Warm up - read once to populate cache
        NodeState warmupRoot = store.getRoot();
        for (String childName : warmupRoot.getChildNodeNames()) {
            NodeState child = warmupRoot.getChildNode(childName);
            for (String subchildName : child.getChildNodeNames()) {
                child.getChildNode(subchildName);
            }
        }

        // Measure cached read time
        long startTime = System.nanoTime();
        NodeState readRoot = store.getRoot();
        int nodeCount = 0;

        for (String childName : readRoot.getChildNodeNames()) {
            nodeCount++;
            NodeState child = readRoot.getChildNode(childName);

            for (String subchildName : child.getChildNodeNames()) {
                nodeCount++;
                child.getChildNode(subchildName);
            }
        }

        long duration = System.nanoTime() - startTime;
        double durationMs = duration / 1_000_000.0;

        System.out.println("=== Warm Read Performance ===");
        System.out.println("Nodes read: " + nodeCount);
        System.out.println("Time: " + durationMs + " ms");
        System.out.println("Avg time per node: " + (durationMs / nodeCount) + " ms");
        System.out.println();
    }

    /**
     * Benchmark 3: Measure property access performance
     * Tests string parsing and type conversion overhead
     */
    @Test
    public void testPropertyAccessPerformance() throws CommitFailedException {
        // Create node with many properties
        NodeBuilder root = store.getRoot().builder();
        NodeBuilder testNode = root.child("test");

        for (int i = 0; i < 100; i++) {
            testNode.setProperty("string" + i, "value" + i);
            testNode.setProperty("long" + i, (long) i);
            testNode.setProperty("bool" + i, i % 2 == 0);
        }

        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Measure property access time
        long startTime = System.nanoTime();
        NodeState testNodeState = store.getRoot().getChildNode("test");

        int propertyCount = 0;
        for (int i = 0; i < 100; i++) {
            testNodeState.getProperty("string" + i);
            testNodeState.getProperty("long" + i);
            testNodeState.getProperty("bool" + i);
            propertyCount += 3;
        }

        long duration = System.nanoTime() - startTime;
        double durationMs = duration / 1_000_000.0;

        System.out.println("=== Property Access Performance ===");
        System.out.println("Properties accessed: " + propertyCount);
        System.out.println("Time: " + durationMs + " ms");
        System.out.println("Avg time per property: " + (durationMs / propertyCount) + " ms");
        System.out.println();
    }

    /**
     * Benchmark 4: Measure write/commit performance
     * Tests the overhead of journal-based commit protocol
     */
    @Test
    public void testCommitPerformance() throws CommitFailedException {
        long totalTime = 0;
        int iterations = 5;

        for (int iter = 0; iter < iterations; iter++) {
            NodeBuilder root = store.getRoot().builder();
            NodeBuilder testNode = root.child("commit_test_" + iter);

            for (int i = 0; i < 10; i++) {
                testNode.child("child" + i).setProperty("value", i);
            }

            long startTime = System.nanoTime();
            store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            long duration = System.nanoTime() - startTime;

            totalTime += duration;
        }

        double avgTimeMs = (totalTime / iterations) / 1_000_000.0;

        System.out.println("=== Commit Performance ===");
        System.out.println("Iterations: " + iterations);
        System.out.println("Avg commit time: " + avgTimeMs + " ms");
        System.out.println();
    }

    /**
     * Benchmark 5: Measure child node enumeration performance
     * Tests getChildNodeNames() efficiency
     */
    @Test
    public void testChildEnumerationPerformance() throws CommitFailedException {
        // Create a node with many children
        NodeBuilder root = store.getRoot().builder();
        NodeBuilder parent = root.child("parent");

        for (int i = 0; i < 100; i++) {
            parent.child("child" + i);
        }

        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store.emptyCaches(); // Clear cache for cold test

        // Measure enumeration time
        long startTime = System.nanoTime();
        NodeState parentState = store.getRoot().getChildNode("parent");

        int childCount = 0;
        for (String childName : parentState.getChildNodeNames()) {
            childCount++;
        }

        long duration = System.nanoTime() - startTime;
        double durationMs = duration / 1_000_000.0;

        System.out.println("=== Child Enumeration Performance ===");
        System.out.println("Children enumerated: " + childCount);
        System.out.println("Time: " + durationMs + " ms");
        System.out.println("Avg time per child: " + (durationMs / childCount) + " ms");
        System.out.println();

        assertEquals(100, childCount);
    }

    /**
     * Benchmark 6: Measure concurrent read performance
     * Tests lock contention and cache efficiency under load
     */
    @Test
    public void testConcurrentReadPerformance() throws Exception {
        // Setup test data
        NodeBuilder root = store.getRoot().builder();
        for (int i = 0; i < 20; i++) {
            root.child("shared" + i).setProperty("data", "value" + i);
        }
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Run concurrent reads
        int threadCount = 10;
        int readsPerThread = 50;
        List<Thread> threads = new ArrayList<>();
        List<Long> durations = new ArrayList<>();

        long overallStart = System.nanoTime();

        for (int t = 0; t < threadCount; t++) {
            Thread thread = new Thread(() -> {
                long threadStart = System.nanoTime();

                for (int i = 0; i < readsPerThread; i++) {
                    NodeState readRoot = store.getRoot();
                    for (String childName : readRoot.getChildNodeNames()) {
                        readRoot.getChildNode(childName);
                    }
                }

                long threadDuration = System.nanoTime() - threadStart;
                synchronized (durations) {
                    durations.add(threadDuration);
                }
            });
            threads.add(thread);
            thread.start();
        }

        // Wait for all threads
        for (Thread thread : threads) {
            thread.join();
        }

        long overallDuration = System.nanoTime() - overallStart;

        double overallMs = overallDuration / 1_000_000.0;
        double avgThreadMs = durations.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0) / 1_000_000.0;

        System.out.println("=== Concurrent Read Performance ===");
        System.out.println("Threads: " + threadCount);
        System.out.println("Reads per thread: " + readsPerThread);
        System.out.println("Total time: " + overallMs + " ms");
        System.out.println("Avg thread time: " + avgThreadMs + " ms");
        System.out.println("Throughput: " + ((threadCount * readsPerThread) / (overallMs / 1000)) + " reads/sec");
        System.out.println();
    }

    /**
     * Benchmark 7: Deep tree traversal
     * Tests cascading lazy loading performance
     */
    @Test
    public void testDeepTreeTraversal() throws CommitFailedException {
        // Create deep tree
        NodeBuilder root = store.getRoot().builder();
        NodeBuilder current = root;
        int depth = 20;

        for (int i = 0; i < depth; i++) {
            current = current.child("level" + i);
            current.setProperty("depth", i);
        }

        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store.emptyCaches();

        // Traverse from root to deepest node
        long startTime = System.nanoTime();
        NodeState currentState = store.getRoot();
        int nodesTraversed = 0;

        for (int i = 0; i < depth; i++) {
            currentState = currentState.getChildNode("level" + i);
            nodesTraversed++;
            assertNotNull(currentState.getProperty("depth"));
        }

        long duration = System.nanoTime() - startTime;
        double durationMs = duration / 1_000_000.0;

        System.out.println("=== Deep Tree Traversal Performance ===");
        System.out.println("Depth: " + depth);
        System.out.println("Nodes traversed: " + nodesTraversed);
        System.out.println("Time: " + durationMs + " ms");
        System.out.println("Avg time per level: " + (durationMs / depth) + " ms");
        System.out.println();
    }
}
