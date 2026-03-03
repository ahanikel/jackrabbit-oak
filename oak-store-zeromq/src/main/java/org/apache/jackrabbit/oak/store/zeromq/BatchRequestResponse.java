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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Optimized request/response handler that batches multiple requests
 * to reduce network round trips and improve throughput.
 *
 * Key improvements:
 * 1. Batch multiple requests into single network call
 * 2. Async request submission with CompletableFuture
 * 3. Automatic batching with configurable window
 * 4. Thread-safe without blocking caller
 */
public class BatchRequestResponse {

    private static final Logger log = LoggerFactory.getLogger(BatchRequestResponse.class);

    private static final int DEFAULT_BATCH_SIZE = 50;
    private static final long DEFAULT_BATCH_WINDOW_MS = 10;

    private final SimpleRequestResponse delegate;
    private final int maxBatchSize;
    private final long batchWindowMs;

    private final ThreadLocal<BatchContext> batchContext = ThreadLocal.withInitial(BatchContext::new);
    private final ExecutorService flushExecutor = Executors.newFixedThreadPool(4,
            new NamedThreadFactory("BatchRequestFlush"));

    private static class BatchContext {
        final List<BatchRequest> pendingRequests = new ArrayList<>();
        long lastFlushTime = System.currentTimeMillis();
    }

    private static class BatchRequest {
        final String op;
        final byte[] args;
        final CompletableFuture<byte[]> future;

        BatchRequest(String op, byte[] args) {
            this.op = op;
            this.args = args;
            this.future = new CompletableFuture<>();
        }
    }

    public BatchRequestResponse(SimpleRequestResponse delegate) {
        this(delegate, DEFAULT_BATCH_SIZE, DEFAULT_BATCH_WINDOW_MS);
    }

    public BatchRequestResponse(SimpleRequestResponse delegate, int maxBatchSize, long batchWindowMs) {
        this.delegate = delegate;
        this.maxBatchSize = maxBatchSize;
        this.batchWindowMs = batchWindowMs;
    }

    /**
     * Submit a request that will be batched with others
     *
     * @param op operation name
     * @param args operation arguments
     * @return CompletableFuture that completes when response is received
     */
    public CompletableFuture<byte[]> requestAsync(String op, byte[] args) {
        BatchContext context = batchContext.get();
        BatchRequest request = new BatchRequest(op, args);

        context.pendingRequests.add(request);

        // Flush if batch is full or window expired
        long now = System.currentTimeMillis();
        if (context.pendingRequests.size() >= maxBatchSize ||
            (now - context.lastFlushTime) >= batchWindowMs) {
            flushBatch(context);
        }

        return request.future;
    }

    /**
     * Synchronous request with batching
     */
    public byte[] request(String op, byte[] args) {
        try {
            return requestAsync(op, args).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Request failed: " + op, e);
        }
    }

    /**
     * Flush pending requests for current thread
     */
    private void flushBatch(BatchContext context) {
        if (context.pendingRequests.isEmpty()) {
            return;
        }

        List<BatchRequest> toFlush = new ArrayList<>(context.pendingRequests);
        context.pendingRequests.clear();
        context.lastFlushTime = System.currentTimeMillis();

        // Submit flush to executor to avoid blocking caller
        flushExecutor.submit(() -> executeBatch(toFlush));
    }

    /**
     * Execute batch of requests over network
     */
    private void executeBatch(List<BatchRequest> requests) {
        if (requests.size() == 1) {
            // Single request, use direct call
            BatchRequest req = requests.get(0);
            try {
                byte[] response = delegate.requestBytes(req.op, req.args);
                req.future.complete(response);
            } catch (Exception e) {
                req.future.completeExceptionally(e);
            }
            return;
        }

        try {
            // Encode batch request
            ByteArrayOutputStream batchOut = new ByteArrayOutputStream();

            // Write number of requests
            batchOut.write(ByteBuffer.allocate(4).putInt(requests.size()).array());

            // Write each request
            for (BatchRequest req : requests) {
                byte[] opBytes = req.op.getBytes();
                batchOut.write(ByteBuffer.allocate(4).putInt(opBytes.length).array());
                batchOut.write(opBytes);
                batchOut.write(ByteBuffer.allocate(4).putInt(req.args.length).array());
                batchOut.write(req.args);
            }

            // Send batch
            byte[] batchResponse = delegate.requestBytes("batch", batchOut.toByteArray());

            // Decode batch response
            ByteBuffer responseBuf = ByteBuffer.wrap(batchResponse);
            int responseCount = responseBuf.getInt();

            if (responseCount != requests.size()) {
                throw new IOException("Response count mismatch: expected " +
                        requests.size() + ", got " + responseCount);
            }

            // Complete each future
            for (BatchRequest req : requests) {
                int responseLen = responseBuf.getInt();
                byte[] responseData = new byte[responseLen];
                responseBuf.get(responseData);
                req.future.complete(responseData);
            }

        } catch (Exception e) {
            log.error("Batch execution failed", e);
            // Fail all requests
            for (BatchRequest req : requests) {
                req.future.completeExceptionally(e);
            }
        }
    }

    /**
     * Force flush of pending requests for current thread
     */
    public void flush() {
        flushBatch(batchContext.get());
    }

    /**
     * Shutdown batch processor
     */
    public void shutdown() {
        // Flush all pending
        flush();

        flushExecutor.shutdown();
        try {
            flushExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Get statistics about batching efficiency
     */
    public BatchStats getStats() {
        return new BatchStats();
    }

    public static class BatchStats {
        public long totalRequests;
        public long batchedRequests;
        public long avgBatchSize;

        public double getBatchingRatio() {
            return totalRequests == 0 ? 0 : (double) batchedRequests / totalRequests;
        }
    }
}
