/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.store.zeromq;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;
import java.util.function.Consumer;

import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

public class LoggingHook implements CommitHook, NodeStateDiff {

    public static final String UUID_NULL = new UUID(0, 0).toString();

    private final Consumer<String> writer;
    private final boolean appendBlobs;

    private LoggingHook(final Consumer<String> writer, boolean appendBlobs) {
        this.writer = writer;
        this.appendBlobs = appendBlobs;
    }

    static LoggingHook newLoggingHook(final Consumer<String> writer, boolean appendBlobs) {
        return new LoggingHook(writer, appendBlobs);
    }

    public void enter(NodeState before, NodeState after) {
    }

    public void leave(NodeState before, NodeState after) {
        writer.accept("n!");
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        writer.accept("p+ " + toString(after));
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        writer.accept("p^ " + toString(after));
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        writer.accept("p- " + toString(before));
        return true;
    }

    private static String safeEncode(String value) {
        try {
            return SafeEncode.safeEncode(value);
        } catch (UnsupportedEncodingException e) {
            return "ERROR: " + e;
        }
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        writer.accept("n+ " + safeEncode(name) + " " + ((ZeroMQNodeState) after).getUuid());
        this.enter(null, after);
        boolean ret = after.compareAgainstBaseState(EmptyNodeState.EMPTY_NODE, this);
        this.leave(null, after);
        return ret;
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        writer.accept("n^ " + safeEncode(name) + " " + ((ZeroMQNodeState) after).getUuid() + " " + ((ZeroMQNodeState) before).getUuid());
        this.enter(before, after);
        boolean ret = after.compareAgainstBaseState(before, this);
        this.leave(before, after);
        return ret;
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        writer.accept("n- " + safeEncode(name));
        return true;
    }

    // TODO: this method should be replaced by one which
    // - checks the size of the value
    // - if the value exceeds a certain limit, stores it as a blob and a reference to that blob
    // - uses the writer directly instead of going via a string
    private static String toString(final PropertyState ps) {
        final StringBuilder val = new StringBuilder();
        val.append(safeEncode(ps.getName()));
        val.append(" <");
        val.append(ps.getType());
        val.append("> ");
        if (ps.getType() == BINARY) {
            val.append("= ");
            try {
                final Blob blob = ps.getValue(BINARY);
                val.append(blob.getReference());
            } catch (Throwable t) {
                val.append(safeEncode(t.getMessage()));
            }
        } else if (ps.getType() == BINARIES) {
            val.append("= [");
            ps.getValue(BINARIES).forEach((Blob b) -> {
                val.append(b.getReference());
                val.append(',');
            });
            replaceOrAppendLastChar(val, ',', ']');
        } else if (ps.isArray()) {
            val.append("= [");
            ps.getValue(STRINGS).forEach((String s) -> {
                val.append(safeEncode(s));
                val.append(',');
            });
            replaceOrAppendLastChar(val, ',', ']');
        } else {
            val.append("= ").append(safeEncode(ps.getValue(STRING)));
        }
        return val.toString();
    }

    private static void replaceOrAppendLastChar(StringBuilder b, char oldChar, char newChar) {
        if (b.charAt(b.length() - 1) == oldChar) {
            b.setCharAt(b.length() - 1, newChar);
        } else {
            b.append(newChar);
        }
    }

    public static void writeBlob(Blob b, Consumer<String> writer) throws IOException {
        synchronized (writer) {
            final int chunkSize = 256 * 1024;
            final byte[] buffer = new byte[chunkSize];
            final Base64.Encoder b64 = Base64.getEncoder();
            String encoded;
            try (final InputStream is = b.getNewStream()) {
                writer.accept("b64+ " + b.getReference());
                int nBytes;
                while ((nBytes = is.read(buffer)) >= 0) {
                    if (nBytes < chunkSize) {
                        encoded = b64.encodeToString(Arrays.copyOf(buffer, nBytes));
                    } else if (nBytes == 0) {
                        Thread.sleep(100);
                        continue;
                    } else {
                        encoded = b64.encodeToString(buffer);
                    }
                    writer.accept("b64d " + encoded);
                }
            } catch (IOException ioe) {
                writer.accept("b64x " + safeEncode(ioe.getMessage()));
                throw ioe;
            } catch (InterruptedException e) {
            } finally {
                writer.accept("b64!");
            }
        }
    }

    private static void appendBlob(StringBuilder sb, Blob blob) {
        final InputStream is = blob.getNewStream();
        final char[] hex = "0123456789ABCDEF".toCharArray();
        int b;
        try {
            while ((b = is.read()) >= 0) {
                sb.append(hex[b >> 4]);
                sb.append(hex[b & 0x0f]);
            }
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @NotNull
    @Override
    public NodeState processCommit(NodeState before, NodeState after, CommitInfo info) {
        writer.accept("R: " + ((ZeroMQNodeState) after).getUuid() + " " + ((ZeroMQNodeState) before).getUuid());
        try {
            after.compareAgainstBaseState(before, this);
        } catch (Throwable t) {
            System.out.println(t.toString());
        }
        writer.accept("R!");
        return after;
    }
}