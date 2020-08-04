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
import org.apache.jackrabbit.oak.spi.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class ZeroMQNodeState extends AbstractNodeState {

    private static final Logger log = LoggerFactory.getLogger(ZeroMQNodeState.class);

    private static final Pattern endChildrenPattern = Pattern.compile("(end children\\n).*", Pattern.DOTALL);
    private static final Pattern endPropertiesPattern = Pattern.compile("(end properties\\n).*", Pattern.DOTALL);
    private static final Pattern tabSeparatedPattern = Pattern.compile("([^\\t]+\\t).*", Pattern.DOTALL);
    private static final Pattern spaceSeparatedPattern = Pattern.compile("([^ ]+ ).*", Pattern.DOTALL);
    private static final Pattern propertyTypePattern = Pattern.compile("([^>]+> = ).*", Pattern.DOTALL);
    private static final Pattern allTheRestPattern = Pattern.compile("(.*)", Pattern.DOTALL);

    protected final ZeroMQNodeStore ns;
    private final String uuid;
    private final Map<String, String> children;
    private final Map<String, ZeroMQPropertyState> properties;
    private final Function<String, ZeroMQNodeState> reader;
    private final Consumer<SerialisedZeroMQNodeState> writer;
    private final String serialised;

    // not private because ZeroMQEmptyNodeState needs it
    ZeroMQNodeState(ZeroMQNodeStore ns, Function<String, ZeroMQNodeState> reader, Consumer<SerialisedZeroMQNodeState> writer) {
        this.ns = ns;
        this.children = new HashMap<>();
        this.properties = new HashMap<>();
        this.reader = reader;
        this.writer = writer;
        serialised = serialise();
        this.uuid = ZeroMQEmptyNodeState.UUID_NULL.toString();
    }

    private ZeroMQNodeState(ZeroMQNodeStore ns, Map<String, String> children, Map<String, ZeroMQPropertyState> properties, String serialised, Function<String, ZeroMQNodeState> reader, Consumer<SerialisedZeroMQNodeState> writer) {
        this.ns = ns;
        this.children = children;
        this.properties = properties;
        this.reader = reader;
        this.writer = writer;
        if (serialised == null) {
            this.serialised = serialise();
        } else {
            this.serialised = serialised;
        }
        try {
            this.uuid = generateUuid();
        } catch (UnsupportedEncodingException ex) {
            throw new IllegalStateException(ex);
        }
    }

    static class ParseFailure extends Exception {
        private ParseFailure(String s) {
            super(s);
        }
    }

    private static final class FinalVar<Type> {
        private Type val;
        private boolean assigned = false;

        public FinalVar() {};
        public void assign(Type val) {
            if (assigned) {
                throw new IllegalStateException("Variable has already been assigned");
            } else {
                this.val = val;
                assigned = true;
            }
        }
        public Type val() {
            if (assigned) {
                return val;
            } else {
                throw new IllegalStateException("Variable has not been assigned yet");
            }
        }
    }

    @FunctionalInterface
    private interface SupplierWithException<T, E extends Exception> {
        T get() throws E;
    }

    private static class Parser {
        private String s;
        private String last;

        private Parser(String s) {
            this.s = s;
            this.last = "";
        }

        private Parser parseString(String t) throws ParseFailure {
            if (!s.startsWith(t)) {
                throw new ParseFailure("Failed to parse " + t);
            }
            s = s.substring(t.length());
            last = t;
            return this;
        }

        private Parser parseLine() throws ParseFailure {
            int nNewLine = s.indexOf('\n');
            last = s.substring(0, nNewLine);
            s = s.substring(nNewLine + 1);
            return this;
        }

        private Parser parseRegexp(Pattern p, int nTrimEnd) throws ParseFailure {
            final Matcher m = p.matcher(s);
            if (m.matches()) {
                last = m.group(1);
                s = s.substring(last.length());
                last = last.substring(0, last.length() - nTrimEnd);
                return this;
            }
            throw new ParseFailure("Failed to parse " + p.pattern());
        }

        private Parser assignTo(FinalVar<String> var) {
            var.assign(last);
            return this;
        }

        private Parser assignToWithDecode(FinalVar<String> var) {
            try {
                var.assign(SafeEncode.safeDecode(last));
            } catch (UnsupportedEncodingException e) {
                throw new IllegalArgumentException(e);
            }
            return this;
        }

        private Parser appendTo(List<String> list) {
            list.add(last);
            return this;
        }

        private Parser appendToValues(List<String> list) {
            try {
                // TODO: BUG: this means that a STRINGS value like [""] is not possible
                if (last.startsWith("[]")) {
                    return this;
                }
                if (last.startsWith("[")) {
                    // array
                    String[] vals = last.substring(1, last.length() - 1).split("[\\[\\],]");
                    for (String val : vals) {
                        list.add(SafeEncode.safeDecode(val));
                    }
                } else {
                    list.add(SafeEncode.safeDecode(last));
                }
            } catch (UnsupportedEncodingException e) {
                throw new IllegalArgumentException(e);
            }
            return this;
        }

        private Parser parseRegexpUntil(SupplierWithException<Parser, ParseFailure> f, Pattern p, int nTrimEnd) throws ParseFailure {
            Parser parser = this;
            Matcher m = p.matcher(parser.s);
            while (!m.matches()) {
                parser = f.get();
                m = p.matcher(parser.s);
            }
            last = m.group(1);
            s = s.substring(last.length());
            last = last.substring(0, last.length() - nTrimEnd);
            return this;
        }
    }

    static ZeroMQNodeState deSerialise(ZeroMQNodeStore ns, String s, Function<String, ZeroMQNodeState> reader, Consumer<SerialisedZeroMQNodeState> writer) throws ParseFailure {
        final List<String> children = new ArrayList<>();
        final List<String> properties = new ArrayList<>();
        final Map<String, String> childrenMap = new HashMap<>();
        final Map<String, ZeroMQPropertyState> propertiesMap = new HashMap<>();
        final Parser parser = new Parser(s);
        parser
            .parseString("begin ZeroMQNodeState\n")
            .parseString("begin children\n")
            .parseRegexpUntil(
                () -> parser.parseLine().appendTo(children),
                endChildrenPattern,
                1
            )
            .parseString("begin properties\n")
            .parseRegexpUntil(
                () -> parser.parseLine().appendTo(properties),
                endPropertiesPattern,
                1
            )
            .parseString("end ZeroMQNodeState\n");
        for (String child : children) {
            Parser p = new Parser(child);
            FinalVar<String> key = new FinalVar<>();
            FinalVar<String> value = new FinalVar<>();
            p
                    .parseRegexp(tabSeparatedPattern, 1).assignTo(key)
                    .parseRegexp(allTheRestPattern, 0).assignTo(value);
            try {
                childrenMap.put(SafeEncode.safeDecode(key.val), value.val);
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException(e);
            }
        }
        for (String prop : properties) {
            FinalVar<String> pName = new FinalVar<>();
            FinalVar<String> pType = new FinalVar<>();
            List<String> pValues   = new ArrayList<>();
            Parser p = new Parser(prop);
            p
                    .parseRegexp(spaceSeparatedPattern, 1).assignToWithDecode(pName)
                    .parseString("<")
                    .parseRegexp(propertyTypePattern, 4).assignTo(pType)
                    .parseRegexp(allTheRestPattern, 0).appendToValues(pValues);
            propertiesMap.put(pName.val(), new ZeroMQPropertyState(ns, pName.val(), pType.val(), pValues));
        }
        final ZeroMQNodeState ret = new ZeroMQNodeState(ns, childrenMap, propertiesMap, s, reader, writer);
        return ret;
    }

    public void serialise(Consumer<SerialisedZeroMQNodeState> writer) {
        writer.accept(new SerialisedZeroMQNodeState(uuid, serialised, this));
    }

    private String serialise() {
        final AtomicReference<Exception> e = new AtomicReference<>();
        final StringBuilder sb = new StringBuilder();
        sb
            .append("begin ZeroMQNodeState\n")
            .append("begin children\n");
        children.forEach((name, uuid) ->
        {
            try {
                sb
                    .append(SafeEncode.safeEncode(name))
                    .append('\t')
                    .append(uuid)
                    .append('\n');
            } catch (UnsupportedEncodingException ex) {
                e.compareAndSet(null, ex);
            }
        });
        if (e.get() != null) {
            throw new IllegalStateException(e.get());
        }
        sb.append("end children\n");
        sb.append("begin properties\n");
        properties.forEach((name, ps) ->
            ps.serialise(sb).append('\n')
        );
        sb.append("end properties\n");
        sb.append("end ZeroMQNodeState\n");
        return sb.toString();
    }

    private String generateUuid() throws UnsupportedEncodingException {
        if (children.isEmpty() && properties.isEmpty()) {
            return ZeroMQEmptyNodeState.UUID_NULL.toString();
        }
        return UUID.nameUUIDFromBytes(serialised.getBytes("UTF-8")).toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ZeroMQNodeState) {
            ZeroMQNodeState that = (ZeroMQNodeState) other;
            if (this.uuid.equals(that.uuid)) {
                return true;
            }
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
            return reader.apply(children.get(name));
        } else {
            return ZeroMQEmptyNodeState.MISSING_NODE(ns, reader, writer);
        }
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
        return new ZeroMQNodeBuilder(this.ns, this, reader, writer);
    }

    public String getUuid() {
        return uuid;
    }

    @Override
    public String toString() {
        return serialised;
    }

    static ZeroMQNodeStateDiffBuilder getNodeStateDiffBuilder(ZeroMQNodeStore ns, ZeroMQNodeState before, Function<String, ZeroMQNodeState> reader, Consumer<SerialisedZeroMQNodeState> writer) {
        return new ZeroMQNodeStateDiffBuilder(ns, before, reader, writer);
    }

    static final class ZeroMQNodeStateDiffBuilder implements NodeStateDiff {

        private final Function<String, ZeroMQNodeState> reader;
        private final Consumer<SerialisedZeroMQNodeState> writer;

        private Map<String, String> children;
        private Map<String, ZeroMQPropertyState> properties;

        private final ZeroMQNodeStore ns;
        private final ZeroMQNodeState before;

        private boolean dirty;

        private ZeroMQNodeStateDiffBuilder(ZeroMQNodeStore ns, ZeroMQNodeState before, Function<String, ZeroMQNodeState> reader, Consumer<SerialisedZeroMQNodeState> writer) {
            this.ns = ns;
            this.reader = reader;
            this.writer = writer;
            this.before = before;
            reset();
        }

        private void reset() {
            this.children = new HashMap<>();
            this.properties = new HashMap<>();
            this.children.putAll(before.children);
            this.properties.putAll(before.properties);
            this.dirty = false;
        }

        public ZeroMQNodeState getNodeState() {
            if (dirty) {
                final ZeroMQNodeState ret = new ZeroMQNodeState(this.ns, this.children, this.properties, null, reader, writer);
                ret.serialise(writer);
                return ret;
            } else {
                return before;
            }
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            properties.put(after.getName(), ZeroMQPropertyState.fromPropertyState(this.ns, after));
            dirty = true;
            return true;
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            properties.remove(before.getName());
            return propertyAdded(after);
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            properties.remove(before.getName());
            dirty = true;
            return true;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            if (after instanceof ZeroMQNodeState) {
                this.children.put(name, ((ZeroMQNodeState) after).getUuid());
            } else {
                final ZeroMQNodeState before = (ZeroMQNodeState) ZeroMQEmptyNodeState.EMPTY_NODE(this.ns, reader, writer);
                final ZeroMQNodeStateDiffBuilder diff = getNodeStateDiffBuilder(this.ns, before, reader, writer);
                after.compareAgainstBaseState(before, diff);
                final ZeroMQNodeState child = diff.getNodeState();
                this.children.put(name, child.getUuid());
            }
            dirty = true;
            return true;
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            //final ZeroMQNodeStateDiffBuilder diff = getNodeStateDiffBuilder(this.ns, (ZeroMQNodeState) before, reader, writer);
            //after.compareAgainstBaseState(before, diff);
            //final ZeroMQNodeState child = diff.getNodeState();
            this.children.remove(name);
            if (after instanceof ZeroMQNodeState) {
                this.children.put(name, ((ZeroMQNodeState) after).getUuid());
            } else {
                final ZeroMQNodeStateDiffBuilder diff = getNodeStateDiffBuilder(this.ns, (ZeroMQNodeState) before, reader, writer);
                after.compareAgainstBaseState(before, diff);
                final ZeroMQNodeState child = diff.getNodeState();
                this.children.put(name, child.getUuid());
            }
            dirty = true;
            return true;
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            this.children.remove(name);
            dirty = true;
            return true;
        }
    }

    public static class SerialisedZeroMQNodeState {

        private final String uuid;
        private final String sNodeState;
        private final ZeroMQNodeState ns;

        public SerialisedZeroMQNodeState(String uuid, String sNodeState, ZeroMQNodeState ns) {
            this.uuid = uuid;
            this.sNodeState = sNodeState;
            this.ns = ns;
        }

        public String getUuid() {
            return uuid;
        }

        public String getserialisedNodeState() {
            return sNodeState;
        }

        public ZeroMQNodeState getNodeState() {
            return ns;
        }

        public String toString() {
            return getserialisedNodeState();
        }
    }
}
