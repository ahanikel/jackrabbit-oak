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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

public class SimpleRequestResponse implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SimpleRequestResponse.class);

    private final String thisInstanceId;
    private final ThreadLocal<ZMQ.Socket> readerSocket;
    private final ThreadLocal<ZMQ.Socket> writerSocket;
    private final ThreadLocal<String> prefixOut;
    private final ThreadLocal<String> prefixIn;
    private final ThreadLocal<String> thisReq;
    private final ThreadLocal<String> lastReq;
    private final ThreadLocal<Long> requestMsgId;
    private final ThreadLocal<Long> responseMsgId;

    public SimpleRequestResponse(Topic topic, String pubAddr, String subAddr) {

        final ZContext context = new ZContext();
        this.thisInstanceId = UUID.randomUUID().toString();
        this.prefixOut = ThreadLocal.withInitial(() -> topic.toString()  + "-req " + thisInstanceId + "-" + Thread.currentThread().getId());
        this.prefixIn  = ThreadLocal.withInitial(() -> topic.toString()  + "-rep " + thisInstanceId + "-" + Thread.currentThread().getId());
        this.thisReq = new ThreadLocal<>();
        this.lastReq = new ThreadLocal<>();
        this.requestMsgId = ThreadLocal.withInitial(() -> 0L);
        this.responseMsgId = ThreadLocal.withInitial(() -> 0L);

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

    private long getThreadLocalRequestMessageId() {
        return requestMsgId.get();
    }

    private byte[] getAndIncThreadLocalRequestMessageIdAsBytes() {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
        long lastMessageId = requestMsgId.get();
        buf.putLong(lastMessageId);
        requestMsgId.set(lastMessageId + 1);
        return buf.array();
    }

    private long getThreadLocalResponseMessageId() {
        return requestMsgId.get();
    }

    private void resetThreadLocalResponseMessageId() {
        responseMsgId.set(0L);
    }

    private void incThreadLocalResponseMessageId() {
        responseMsgId.set(responseMsgId.get() + 1);
    }

    public byte[] requestBytes(String op, byte[] args) {
        lastReq.set(thisReq.get());
        thisReq.set(op + " " + args);
        ZMQ.Socket writer = writerSocket.get();
        ZMQ.Socket reader = readerSocket.get();
        byte[] ret;
        byte[] msgid = getAndIncThreadLocalRequestMessageIdAsBytes();
        resetThreadLocalResponseMessageId();
        byte[] reqId;
        byte[] repId;
        do {
            do {
                writer.sendMore(prefixOut.get());
                writer.sendMore(msgid);
                writer.sendMore(op);
                writer.send(args);
                ret = reader.recv();
            } while (ret == null); // ret will contain the response type plus thread id
            reqId = reader.recv();
            repId = reader.recv();
            if (!Arrays.equals(reqId, msgid)) {
                log.warn("Request id does not match, actual: {}, expected: {}", reqId, msgid);
                while (reader.recv() != null); // flush garbage
            }
        } while (!Arrays.equals(reqId, msgid));
        if (!Arrays.equals(repId, Util.LONG_ZERO)) {
            log.error("Reply id does not match, actual: {}", repId);
        }
        return reader.recv();
    }

    public String receiveMore() {
        ZMQ.Socket reader = readerSocket.get();
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
