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

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

import static org.apache.jackrabbit.oak.store.zeromq.SafeEncode.safeEncode;

// this class is only used by ImportToSimpleCommand and should be replaced
// by SimpleNodeStateDiffGenerator, if possible
public class SimpleNodeStateStore implements NodeStateStore {

    private final BlobStore blobStore;

    public SimpleNodeStateStore(BlobStore blobStore) {
        this.blobStore = blobStore;
    }

    @Override
    public String getNodeState(String ref) throws IOException {
        return blobStore.getString(ref);
    }

    @Override
    public boolean hasNodeState(String ref) throws IOException {
        try {
            return getNodeState(ref) != null;
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    private void writeLine(OutputStream os, String s) throws IOException {
        os.write(s.getBytes());
        os.write('\n');
    }

    private String serializePropertyState(BlobStore blobStore, PropertyState ps) throws IOException {
        final StringBuilder sb = new StringBuilder();
        sb
            .append(safeEncode(ps.getName()))
            .append(" <")
            .append(ps.getType().toString())
            .append("> ");
        if (ps.getType().equals(Type.BINARY)) {
            final Blob blob = ps.getValue(Type.BINARY);
            if (blob instanceof SimpleBlob) {
                sb.append(blob.getReference());
            } else {
                String ref;
                try {
                    ref = blobStore.putInputStream(blob.getNewStream());
                } catch (BlobAlreadyExistsException e) {
                    ref = e.getRef();
                }
                sb.append(ref);
            }
        } else if (ps.getType().equals(Type.BINARIES)) {
            sb.append('[');
            final Iterable<Blob> blobs = ps.getValue(Type.BINARIES);
            for (Blob blob : blobs) {
                if (blob instanceof SimpleBlob) {
                    sb.append(blob.getReference());
                } else {
                    String ref;
                    try {
                        ref = blobStore.putInputStream(blob.getNewStream());
                    } catch (BlobAlreadyExistsException e) {
                        ref = e.getRef();
                    }
                    sb.append(ref);
                }
                sb.append(',');
            }
            if (sb.charAt(sb.length() - 1) == ',') {
                sb.deleteCharAt(sb.length() - 1);
            }
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

    @Override
    public SimpleNodeState putNodeState(NodeState ns) throws IOException {
        return putNodeState(ns, null);
    }

    public SimpleNodeState putNodeState(NodeState ns, SimpleNodeStore simpleNodeStore) throws IOException {
        if (ns instanceof SimpleNodeState) {
            return (SimpleNodeState) ns;
        }

        final List<String> children = new ArrayList<>();
        final List<String> properties = new ArrayList<>();
        final Map<String, String> childrenMap = new HashMap<>();
        final Map<String, String> propertiesMap = new HashMap<>();

        ns.compareAgainstBaseState(EmptyNodeState.EMPTY_NODE, new NodeStateDiff() {

            @Override
            public boolean propertyAdded(PropertyState after) {
                try {
                    String ps = serializePropertyState(blobStore, after);
                    properties.add("p+ " + ps);
                    propertiesMap.put(after.getName(), ps);
                    return true;
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public boolean childNodeAdded(String name, NodeState after) {
                try {
                    SimpleNodeState child = putNodeState(after, simpleNodeStore);
                    children.add("n+ " + safeEncode(name) + " " + child.getRef());
                    childrenMap.put(name, child.getRef());
                    return true;
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
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

        final File tempFile = blobStore.getTempFile();
        try (OutputStream os = new FileOutputStream(tempFile)) {
            writeLine(os, "n:");
            for (String c : children) {
                writeLine(os, c);
            }
            for (String p : properties) {
                writeLine(os, p);
            }
            writeLine(os, "n!");
        }
        String ref;
        try {
            ref = blobStore.putTempFile(tempFile);
        } catch (BlobAlreadyExistsException e) {
            ref = e.getRef();
        }
        return SimpleNodeState.get(simpleNodeStore, ref, childrenMap, propertiesMap);
    }

    public BlobStore getBlobStore() {
        return blobStore;
    }
}
