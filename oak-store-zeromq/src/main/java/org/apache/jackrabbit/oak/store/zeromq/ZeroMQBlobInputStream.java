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
import org.zeromq.ZMQ;

import java.io.InputStream;
import java.util.function.Supplier;

public class ZeroMQBlobInputStream extends InputStream {
    byte[] buffer;
    private int cur = 0;
    private int max = 0;
    private int offset = 0;
    private volatile boolean init = false;
    private volatile boolean error = false;
    private final Supplier<ZMQ.Socket> blobReader;
    private ZMQ.Socket reader;
    private final String reference;
    private String verb = "";

    private static final Logger log = LoggerFactory.getLogger(ZeroMQBlobInputStream.class.getName());

    ZeroMQBlobInputStream(Supplier<ZMQ.Socket> blobReader, String reference) {
        this.blobReader = blobReader;
        this.reference = reference;
    }

    private void init() {
        if (!init) {
            reader = blobReader.get();
            try {
                init = true;
                buffer = new byte[1024 * 1024]; // not final because of fear it's not being GC'd
            } catch (Throwable t) {
                log.error(t.getMessage());
                error = true;
            }
        }
    }

    @Override
    public synchronized int read() {
        if (error) {
            return -1;
        }
        init();
        if (cur == max) {
            nextBunch();
        }
        if (max == 0) {
            return -1;
        }
        if (max < 0) {
            error = true;
            throw new IllegalStateException();
        }
        return 0x000000ff & buffer[cur++];
    }

    private void nextBunch() {
        if (reader != blobReader.get()) {
            throw new IllegalStateException("*** Reading thread has changed! ***");
        }
        reader.send("blob " + reference + " " + offset + " " + buffer.length);
        verb = reader.recvStr();
        max = reader.recv(buffer, 0, buffer.length, 0);
        if (verb.equals("N")) {
            log.error("Blob " + reference + " not found");
            error = true;
        } else if (verb.equals("F")) {
            log.error("When fetching blob " + reference + ": " + new String(buffer, 0, max));
            error = true;
        } else {
            offset += max;
            cur = 0;
        }
    }
}
