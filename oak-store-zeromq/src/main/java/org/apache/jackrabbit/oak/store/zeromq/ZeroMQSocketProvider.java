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

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class ZeroMQSocketProvider implements Supplier<ZMQ.Socket>, Closeable {

    private final String url;
    private final ZContext context;
    private final SocketType socketType;
    private final Map<Long, ZMQ.Socket> sockets = new ConcurrentHashMap<Long, ZMQ.Socket>();

    public ZeroMQSocketProvider(String url, ZContext context, SocketType socketType) {
        this.url = url;
        this.context = context;
        this.socketType = socketType;
    }

    @Override
    public ZMQ.Socket get() {
        long threadId = Thread.currentThread().getId();
        ZMQ.Socket socket = sockets.get(threadId);
        if (socket == null) {
            synchronized (sockets) {
                socket = sockets.get(threadId);
                if (socket == null) {
                    socket = context.createSocket(socketType);
                    socket.connect(url);
                    sockets.put(threadId, socket);
                }
            }
        }
        return socket;
    }

    @Override
    public void close() {
        for (ZMQ.Socket socket : sockets.values()) {
            socket.close();
        }
    }
}
