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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

public class SimpleNodeStoreSerialiser {

    private final Consumer<String> writer;

    public SimpleNodeStoreSerialiser(Consumer<String> writer) {
        this.writer = writer;
    }

    public void run(NodeState ns, String path) {

        final List<String> children = new ArrayList<>();
        final List<String> properties = new ArrayList<>();

        ns.compareAgainstBaseState(EmptyNodeState.EMPTY_NODE, new NodeStateDiff() {

            @Override
            public boolean propertyAdded(PropertyState after) {
                try {
                    properties.add("p+ " + serializePropertyState(after));
                    return true;
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public boolean childNodeAdded(String name, NodeState after) {
                children.add(safeEncode(name));
                return true;
            }

            @Override
            public boolean propertyChanged(PropertyState before, PropertyState after) {
                throw new IllegalStateException("This should not happen");
            }

            @Override
            public boolean propertyDeleted(PropertyState before) {
                throw new IllegalStateException("This should not happen");
            }

            @Override
            public boolean childNodeChanged(String name, NodeState before, NodeState after) {
                throw new IllegalStateException("This should not happen");
            }

            @Override
            public boolean childNodeDeleted(String name, NodeState before) {
                throw new IllegalStateException("This should not happen");
            }
        });

        children.sort(Comparator.naturalOrder());
        properties.sort(Comparator.naturalOrder());

        write("n: ");
        writeLine(path);

        for (String c : children) {
            write("n+ ");
            writeLine(safeEncode(c));
        }
        for (String p : properties) {
            writeLine(p);
        }
        writeLine("n!");
        for (String c : children) {
            NodeState child = ns.getChildNode(c);
            run(child, path + "/" + c);
        }
    }

    private String serializePropertyState(PropertyState ps) throws IOException {
        final StringBuilder sb = new StringBuilder();
        sb
            .append(safeEncode(ps.getName()))
            .append(" <")
            .append(ps.getType().toString())
            .append("> ");
        if (ps.getType().equals(Type.BINARY)) {
            sb.append('-');
        } else if (ps.getType().equals(Type.BINARIES)) {
            sb.append('[');
            sb.append(ps.count());
            sb.append(']');
        } else if (ps.isArray()) {
            sb.append('[');
            final Iterable<String> strings = ps.getValue(Type.STRINGS);
            for (String s : strings) {
                sb.append(SafeEncode.safeEncode(s));
                sb.append(',');
            }
            if (sb.charAt(sb.length() - 1) == ',') {
                sb.deleteCharAt(sb.length() - 1);
            }
            sb.append(']');
        } else {
            sb.append(SafeEncode.safeEncode(ps.getValue(Type.STRING)));
        }
        return sb.toString();
    }

    private void write(String s) {
        writer.accept(s);
    }

    private void writeLine() {
        write("\n");
    }

    private void writeLine(String s) {
        write(s);
        write("\n");
    }

    private String safeEncode(String s) {
        try {
            return SafeEncode.safeEncode(s);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }
}
