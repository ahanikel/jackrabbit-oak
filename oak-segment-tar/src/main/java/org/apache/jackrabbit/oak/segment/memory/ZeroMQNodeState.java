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
package org.apache.jackrabbit.oak.segment.memory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.segment.util.SafeEncode;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class ZeroMQNodeState extends AbstractNodeState {

    private final UUID uuid;

    private final Map<String, UUID> children;

    private final Map<String, ZeroMQPropertyState> properties;

    public static ZeroMQNodeState newZeroMQNodeState(UUID uuid, Function<String, String> reader) {
        final ZeroMQNodeState zmqNodeState = new ZeroMQNodeState(uuid, reader);
        zmqNodeState.init();
        return zmqNodeState;
    }

    private final Function<String, String> reader;

    private ZeroMQNodeState(UUID uuid, Function<String, String> reader) {
        this.uuid = uuid;
        this.children = new HashMap<>();
        this.properties = new HashMap<>();
        this.reader = reader;
    }

    private void init() {
    }

    public void serialise(Consumer<StringBuilder> writer) {
        final StringBuilder sb = new StringBuilder();
        sb
            .append("begin ZeroMQNodeState ")
            .append(uuid.toString())
            .append('\n')
            .append("begin children\n");
        children.forEach((name, uuid) ->
            sb
                .append(name)
                .append('\t')
                .append(uuid.toString())
                .append('\n'));
        sb.append("end children\n");
        sb.append("begin properties\n");
        properties.forEach((name, ps) ->
            serialisePropertyState(ps, sb).append('\n')
        );
        sb.append("end properties\n");
        sb.append("end ZeroMQNodeState\n");
        writer.accept(sb);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ZeroMQNodeState) {
            return this.uuid.equals(((ZeroMQNodeState) other).uuid);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 79 * hash + Objects.hashCode(this.uuid);
        return hash;
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return properties.values();
    }

    @Override
    public boolean hasChildNode(String name) {
        return children.containsKey(name);
    }

    @Override
    public NodeState getChildNode(String name) throws IllegalArgumentException {
        if (children.containsKey(name)) {
            return new ZeroMQNodeState(children.get(name), reader);
        }
        throw new IllegalArgumentException("No such child");
    }

    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return () -> {
            Stream s = children.entrySet().stream().map(child -> new ChildNodeEntry() {
                @Override
                public String getName() {
                    return child.getKey();
                }

                @Override
                public NodeState getNodeState() {
                    return ZeroMQNodeState.this.getChildNode(child.getKey());
                }
            });
            return s.iterator();
        };
    }

    @Override
    public NodeBuilder builder() {
        return new MemoryNodeBuilder(this);
    }

    // TODO: copy-pasted from LoggingHook.java
    private static StringBuilder serialisePropertyState(final PropertyState ps, final StringBuilder sb) {
        sb.append(safeEncode(ps.getName()));
        sb.append(" <");
        sb.append(ps.getType());
        sb.append("> ");
        if (ps.getType() == BINARY) {
            sb.append("= ");
            final Blob blob = ps.getValue(BINARY);
            appendBlob(sb, blob);
        } else if (ps.getType() == BINARIES) {
            sb.append("= [");
            ps.getValue(BINARIES).forEach((Blob b) -> {
                appendBlob(sb, b);
                sb.append(',');
            });
            replaceOrAppendLastChar(sb, ',', ']');
        } else if (ps.isArray()) {
            sb.append("= [");
            ps.getValue(STRINGS).forEach((String s) -> {
                sb.append(safeEncode(s));
                sb.append(',');
            });
            replaceOrAppendLastChar(sb, ',', ']');
        } else {
            sb.append("= ").append(safeEncode(ps.getValue(STRING)));
        }
        return sb;
    }

    private static void replaceOrAppendLastChar(StringBuilder b, char oldChar, char newChar) {
        if (b.charAt(b.length() - 1) == oldChar) {
            b.setCharAt(b.length() - 1, newChar);
        } else {
            b.append(newChar);
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

    private static String safeEncode(String value) {
        try {
            return SafeEncode.safeEncode(value);
        } catch (UnsupportedEncodingException e) {
            return "ERROR: " + e;
        }
    }
}
