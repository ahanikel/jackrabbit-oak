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
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;

public class SimpleRequestResponse implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SimpleRequestResponse.class);
    private static final String processId = ManagementFactory.getRuntimeMXBean().getName();

    private final ThreadLocal<ZMQ.Socket> readerSocket;
    private final ThreadLocal<ZMQ.Socket> writerSocket;
    private final ThreadLocal<String> prefixOut;
    private final ThreadLocal<String> prefixIn;
    private final ThreadLocal<String> thisReq;
    private final ThreadLocal<String> lastReq;
    private final ThreadLocal<Long> msgid;

    public SimpleRequestResponse(Topic topic, String pubAddr, String subAddr) {

        final ZContext context = new ZContext();
        this.prefixOut = ThreadLocal.withInitial(() -> topic.toString()  + "-req " + processId + "-" + Thread.currentThread().getId());
        this.prefixIn  = ThreadLocal.withInitial(() -> topic.toString()  + "-rep " + processId + "-" + Thread.currentThread().getId());
        this.thisReq = new ThreadLocal<>();
        this.lastReq = new ThreadLocal<>();
        this.msgid = ThreadLocal.withInitial(() -> 0L);

        this.readerSocket = ThreadLocal.withInitial(() -> {
            ZMQ.Socket ret = context.createSocket(SocketType.SUB);
            ret.subscribe(prefixIn.get());
            ret.setReceiveTimeOut(1000);
            ret.connect(subAddr);
            return ret;
        });

        this.writerSocket = ThreadLocal.withInitial(() -> {
            ZMQ.Socket ret = context.createSocket(SocketType.PUB);
            ret.connect(pubAddr);
            return ret;
        });
    }

    public String requestString(String op, byte[] msg) {
        return new String(requestBytes(op, msg));
    }

    public String requestString(String op, String msg) {
        return requestString(op, msg.getBytes());
    }

    private byte[] getThreadLocalMessageId() {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
        long lastMessageId = msgid.get();
        buf.putLong(lastMessageId);
        msgid.set(lastMessageId + 1);
        return buf.array();
    }

    public byte[] requestBytes(String op, byte[] args) {
        lastReq.set(thisReq.get());
        thisReq.set(op + " " + args);
        final ZMQ.Socket writer = writerSocket.get();
        final ZMQ.Socket reader = readerSocket.get();
        byte[] ret;
        byte[] msgid = getThreadLocalMessageId();
        do {
            writer.sendMore(prefixOut.get());
            writer.sendMore(msgid);
            writer.sendMore(op);
            writer.send(args);
            ret = reader.recv();
        } while (ret == null);
        return reader.recv();
    }

    public String receiveMore() {
        return readerSocket.get().recvStr();
    }

    public int receiveMore(byte[] buffer, int offset, int len, int flags) {
        return readerSocket.get().recv(buffer, offset, len, flags);
    }

    public String getLastReq() {
        return lastReq.get();
    }

    @Override
    public void close() {
        // Don't know how to iterate over ThreadLocal objects
    }

    public enum Topic {
        READ("read"),
        WRITE("write"),
        JOURNAL("journal");

        private final String t;

        Topic(String t) {
            this.t = t;
        }

        @Override
        public String toString() {
            return t;
        }
    }
}
