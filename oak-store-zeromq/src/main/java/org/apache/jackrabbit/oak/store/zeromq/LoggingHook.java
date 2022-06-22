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
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

public class LoggingHook implements CommitHook, NodeStateDiff {

    public static final Logger log = LoggerFactory.getLogger(LoggingHook.class);

    public static final String UUID_NULL = new UUID(0, 0).toString();

    private final BiConsumer<String, String> writer;

    private LoggingHook(final BiConsumer<String, String> writer) {
        this.writer = writer;
    }

    static LoggingHook newLoggingHook(final BiConsumer<String, String> writer) {
        return new LoggingHook(writer);
    }

    public void enter(NodeState before, NodeState after) {
    }

    public void leave(NodeState before, NodeState after) {
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        writer.accept("p+", toString(after));
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        writer.accept("p^", toString(after));
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        writer.accept("p-", toString(before));
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
        writer.accept("n+", safeEncode(name) + " " + ((SimpleNodeState) after).getRef());
        return true;
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        writer.accept("n^", safeEncode(name) + " " + ((SimpleNodeState) after).getRef() + " " + ((SimpleNodeState) before).getRef());
        return true;
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        writer.accept("n-", safeEncode(name));
        return true;
    }

    // TODO: this method should be replaced by one which
    // - checks the size of the value
    // - if the value exceeds a certain limit, stores it as a blob and a reference to that blob
    // - uses the writer directly instead of going via a string
    private static String toString(final PropertyState ps) {
        if (!(ps instanceof SimplePropertyState)) {
            final String msg = String.format("ps has class %1$s", ps.getClass().getName());
            log.error(msg);
            throw new IllegalStateException(msg);
        }
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

    public static void writeBlob(Blob b, BiConsumer<String, String> writer) throws IOException {
        try (InputStream is = b.getNewStream()) {
            writeBlob(b.getReference(), is, writer);
        }
    }

    public static void writeBlob(String ref, InputStream is, BiConsumer<String, String> writer) {
        synchronized (writer) {
            final int chunkSize = 256 * 1024;
            byte[] buffer = new byte[chunkSize]; // not final because of fear it's not being GC'd
            final Base64.Encoder b64 = Base64.getEncoder();
            String encoded;
            writer.accept("b64+", ref);
            try  {
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
                    // TODO: check if we still need this
                    if (ref.equals("2E79040765B71C748E4641664489CAD5")) {
                        // One could think that even though this message isn't exactly 16384 bytes long
                        // there is something special about this size.
                        // I guess it's 16384 when the zmq header is added.
                        writer.accept("b64d", encoded + " ");
                    } else {
                        writer.accept("b64d", encoded);
                    }
                }
            } catch (InterruptedException e) {
            } catch (Exception ioe) {
                writer.accept("b64x", safeEncode(ioe.getMessage()));
            } finally {
                writer.accept("b64!", "");
            }
        }
    }

    @NotNull
    @Override
    public NodeState processCommit(NodeState before, NodeState after, @Nullable CommitInfo info) {
        writer.accept("n:", ((SimpleNodeState) after).getRef() + " " + ((SimpleNodeState) before).getRef());
        try {
            after.compareAgainstBaseState(before, this);
        } catch (Throwable t) {
            System.out.println(t.toString());
        }
        writer.accept("n!", "");
        return after;
    }
}
