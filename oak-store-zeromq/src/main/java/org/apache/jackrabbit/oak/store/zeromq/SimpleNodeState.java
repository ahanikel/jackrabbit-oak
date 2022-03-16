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

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.store.zeromq.SafeEncode.safeDecode;
import static org.apache.jackrabbit.oak.store.zeromq.SafeEncode.safeEncode;

public class SimpleNodeState implements NodeState {

    public static final UUID UUID_NULL = new UUID(0L, 0L);
    public static final SimpleNodeState EMPTY = new SimpleNodeState(true);

    private final Function<String, InputStream> loader;
    private final String ref;
    private Map<String, String> children;
    private Map<String, String> properties;
    private boolean loaded;
    private boolean exists;

    private SimpleNodeState(boolean exists) {
        this.loader = null;
        this.ref = UUID_NULL.toString();
        this.children = ImmutableMap.of();
        this.properties = ImmutableMap.of();
        this.loaded = true;
        this.exists = exists;
    }

    private SimpleNodeState(Function<String, InputStream> loader, String ref) {
        this.loader = loader;
        this.ref = ref;
        this.loaded = false;
        this.exists = true;
    }

    private void ensureLoaded() {
        if (!loaded) {
            synchronized (this) {
                Pair<Map<String, String>, Map<String, String>> p;
                try {
                    p = deserialise(loader.apply(ref));
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
                children = p.fst;
                properties = p.snd;
                loaded = true;
            }
        }
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that instanceof SimpleNodeState) {
            return ref.equals(((SimpleNodeState) that).ref);
        } else if (that instanceof NodeState) {
            return AbstractNodeState.equals(this, (NodeState) that);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return ref.hashCode();
    }

    public String getRef() {
        return ref;
    }

    public static SimpleNodeState get(Function<String, InputStream> loader, String ref) {
        return new SimpleNodeState(loader, ref);
    }

    public static void serialise(OutputStream os, Map<String, String> children, Map<String, String> properties) throws IOException {
        final List<String> cs = new ArrayList<>();
        final List<String> ps = new ArrayList<>();

        for (Map.Entry<String, String> entry : children.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            cs.add("n+ " + safeEncode(key) + " " + value);
        }

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String name = entry.getKey();
            String rest = entry.getValue();
            ps.add("p+ " + safeEncode(name) + " " + rest);
        }

        // The order is important: equal nodestates must result in equal serialisation output,
        // so they have the same hash value.
        cs.sort(Comparator.naturalOrder());
        ps.sort(Comparator.naturalOrder());

        writeLine(os, "n:");
        for (String c : cs) {
            writeLine(os, c);
        }
        for (String p : ps) {
            writeLine(os, p);
        }
        writeLine(os, "n!");
    }

    public static Pair<Map<String, String>, Map<String, String>> deserialise(InputStream is) throws IOException {
        final Map<String, String> children = new HashMap<>();
        final Map<String, String> properties = new HashMap<>();
        for (String line : IOUtils.readLines(is, Charset.defaultCharset())) {
            StringTokenizer st = new StringTokenizer(line);
            String op = st.nextToken();
            switch(op) {
                case "n+": children.put(safeDecode(st.nextToken()), st.nextToken()); break;
                case "p+": properties.put(safeDecode(st.nextToken()), st.nextToken()); break;
            }
        }
        return Pair.of(children, properties);
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public boolean hasProperty(@NotNull String name) {
        ensureLoaded();
        return properties.containsKey(name);
    }

    @Override
    public @Nullable PropertyState getProperty(@NotNull String name) {
        ensureLoaded();
        return SimplePropertyState.deserialise(properties.get(name));
    }

    @Override
    public boolean getBoolean(@NotNull String name) {
        PropertyState property = getProperty(name);
        return property != null
            && property.getType() == Type.BOOLEAN
            && property.getValue(Type.BOOLEAN);
    }

    @Override
    public long getLong(String name) {
        PropertyState property = getProperty(name);
        if (property != null && property.getType() == Type.LONG) {
            return property.getValue(Type.LONG);
        } else {
            return 0;
        }
    }

    @Override
    public @Nullable String getString(String name) {
        PropertyState property = getProperty(name);
        if (property != null && property.getType() == Type.STRING) {
            return property.getValue(Type.STRING);
        } else {
            return null;
        }
    }

    @Override
    public @NotNull Iterable<String> getStrings(@NotNull String name) {
        PropertyState property = getProperty(name);
        if (property != null && property.getType() == Type.STRINGS) {
            return property.getValue(Type.STRINGS);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public @Nullable String getName(@NotNull String name) {
        PropertyState property = getProperty(name);
        if (property != null && property.getType() == Type.NAME) {
            return property.getValue(Type.NAME);
        } else {
            return null;
        }
    }

    @Override
    public @NotNull Iterable<String> getNames(@NotNull String name) {
        PropertyState property = getProperty(name);
        if (property != null && property.getType() == Type.NAMES) {
            return property.getValue(Type.NAMES);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public long getPropertyCount() {
        ensureLoaded();
        return properties.size();
    }

    @Override
    public @NotNull Iterable<? extends PropertyState> getProperties() {
        ensureLoaded();
        return
            properties.values()
            .stream()
            .map(SimplePropertyState::deserialise)
            .collect(Collectors.toList());
    }

    @Override
    public boolean hasChildNode(@NotNull String name) {
        ensureLoaded();
        return children.containsKey(name);
    }

    @Override
    public @NotNull NodeState getChildNode(@NotNull String name) throws IllegalArgumentException {
        ensureLoaded();
        return get(loader, children.get(name));
    }

    @Override
    public long getChildNodeCount(long max) {
        ensureLoaded();
        return children.size();
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        ensureLoaded();
        return children.keySet();
    }

    @Override
    public @NotNull Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        ensureLoaded();
        return children.entrySet().stream().map(e1 -> new ChildNodeEntry() {
            @Override
            public boolean equals(Object e2) {
                if (!(e2 instanceof ChildNodeEntry)) {
                    return false;
                }
                final ChildNodeEntry other = (ChildNodeEntry) e2;
                final NodeState otherNodeState = other.getNodeState();
                if (!(otherNodeState instanceof SimpleNodeState)) {
                    return false;
                }
                final SimpleNodeState sns = (SimpleNodeState) other.getNodeState();
                return e1.getValue().equals(sns.getRef());
            }

            @Override
            public int hashCode() {
                return getNodeState().getRef().hashCode();
            }

            @Override
            public @NotNull String getName() {
                return e1.getKey();
            }

            @Override
            public @NotNull SimpleNodeState getNodeState() {
                return SimpleNodeState.get(loader, e1.getValue());
            }
        }).collect(Collectors.toList());
    }

    @Override
    public @NotNull NodeBuilder builder() {
        return new SimpleNodeBuilder(this);
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        return AbstractNodeState.compareAgainstBaseState(this, base, diff);
    }

    private static void writeLine(OutputStream os, String s) throws IOException {
        os.write(s.getBytes());
        os.write('\n');
    }
}
