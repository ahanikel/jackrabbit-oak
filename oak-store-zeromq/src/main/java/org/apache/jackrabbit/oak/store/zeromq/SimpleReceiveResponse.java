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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.concurrent.TimeUnit;

public class SimpleReceiveResponse extends Thread {

    private final String pubAddr;
    private final String subAddr;
    private final Cache<String, TransactionContext> cache;

    public SimpleReceiveResponse(String pubAddr, String subAddr) {
        this.pubAddr = pubAddr;
        this.subAddr = subAddr;
        this.cache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();
    }

    @Override
    public void run() {
        ZContext context = new ZContext();
        ZMQ.Socket pub = context.createSocket(SocketType.PUB);
        pub.connect(pubAddr);
        ZMQ.Socket sub = context.createSocket(SocketType.SUB);
        sub.subscribe("");
        sub.connect(subAddr);

        while (!Thread.currentThread().isInterrupted()) {

        }

        sub.close();
        context.close();
    }

    private static class TransactionContext {
        private long msgid;
        private TransactionContext() {
        }
    }
}
