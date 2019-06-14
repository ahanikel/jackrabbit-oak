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

    private final String uuid;
    private final Map<String, String> children;
    private final Map<String, ZeroMQPropertyState> properties;
    private final Function<String, ZeroMQNodeState> reader;
    private final Consumer<String> writer;

    ZeroMQNodeState(String uuid, Function<String, ZeroMQNodeState> reader, Consumer<String> writer) {
        this.uuid = uuid;
        this.children = new HashMap<>();
        this.properties = new HashMap<>();
        this.reader = reader;
        this.writer = writer;
    }

    private ZeroMQNodeState(String uuid, Map<String, String> children, Map<String, ZeroMQPropertyState> properties, Function<String, ZeroMQNodeState> reader, Consumer<String> writer) {
        this.uuid = uuid;
        this.children = children;
        this.properties = properties;
        this.reader = reader;
        this.writer = writer;
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

        private Parser parseRegexp(String re) throws ParseFailure {
            final Pattern p = Pattern.compile(re, Pattern.DOTALL);
            final Matcher m = p.matcher(s);
            if (m.matches()) {
                last = m.group(1);
                s = m.group(2);
                return this;
            }
            throw new ParseFailure("Failed to parse " + re);
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

        private Parser appendToWithDecode(List<String> list) {
            try {
                list.add(SafeEncode.safeDecode(last));
            } catch (UnsupportedEncodingException e) {
                throw new IllegalArgumentException(e);
            }
            return this;
        }

        private Parser parseRegexpUntil(SupplierWithException<Parser, ParseFailure> f, String until) throws ParseFailure {
            Parser parser = this;
            final Pattern p = Pattern.compile(until, Pattern.DOTALL);
            Matcher m = p.matcher(parser.s);
            while (!m.matches()) {
                parser = f.get();
                m = p.matcher(parser.s);
            }
            last = m.group(1);
            s = m.group(2);
            return this;
        }
    }

    static String parseUuidFromSerialisedNodeState(String s) throws Exception {
        final Parser parser = new Parser(s);
        final FinalVar<String> ret = new FinalVar<>();
        parser
                .parseRegexp("begin ZeroMQNodeState ([^\\n]+)\\n(.*)")
                .assignTo(ret);
        return ret.val();
    }

    static ZeroMQNodeState deSerialise(String s, Function<String, ZeroMQNodeState> reader, Consumer<String> writer) throws ParseFailure {
        final FinalVar<String> id = new FinalVar();
        final List<String> children = new ArrayList<>();
        final List<String> properties = new ArrayList<>();
        final Parser parser = new Parser(s);
        parser
                .parseRegexp("begin ZeroMQNodeState ([^\\n]+)\\n(.*)")
                .assignTo(id)
                .parseString("begin children\n")
                .parseRegexpUntil(
                        () -> parser.parseRegexp("([^\\n]*)\\n(.*)").appendTo(children),
                       "(end children)\\n(.*)"
                )
                .parseString("begin properties\n")
                .parseRegexpUntil(
                        () -> parser.parseRegexp("([^\\n]*)\\n(.*)").appendTo(properties),
                        "(end properties)\\n(.*)"
                )
                .parseString("end ZeroMQNodeState\n");
        final ZeroMQNodeState ret = new ZeroMQNodeState(id.val(), reader, writer);
        for (String child : children) {
            Parser p = new Parser(child);
            FinalVar<String> key = new FinalVar<>();
            FinalVar<String> value = new FinalVar<>();
            p
                    .parseRegexp("([^\\t]+)\\t(.*)").assignTo(key)
                    .parseRegexp("(.*)(.*)").assignTo(value);
            try {
                ret.children.put(SafeEncode.safeDecode(key.val), value.val);
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
                    .parseRegexp("([^ ]+) (.*)").assignToWithDecode(pName)
                    .parseRegexp("<([^>]+)> = (.*)").assignTo(pType)
                    .parseRegexp("(.*)(.*)").appendToWithDecode(pValues);
            ret.properties.put(pName.val(), new ZeroMQPropertyState(pName.val(), pType.val(), pValues));
        }
        return ret;
    }

    public void serialise(Consumer<String> writer) {
        final AtomicReference<Exception> e = new AtomicReference<>();
        final StringBuilder sb = new StringBuilder();
        sb
            .append("begin ZeroMQNodeState ")
            .append(uuid)
            .append('\n')
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
        writer.accept(sb.toString());
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
            return reader.apply(children.get(name));
        } else {
            return ZeroMQEmptyNodeState.MISSING_NODE(reader, writer);
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
        return new ZeroMQNodeBuilder(this, reader, writer);
    }

    public String getUuid() {
        return uuid;
    }

    static ZeroMQNodeStateDiffBuilder getNodeStateDiffBuilder(ZeroMQNodeState before, Function<String, ZeroMQNodeState> reader, Consumer<String> writer) {
        return new ZeroMQNodeStateDiffBuilder(before, reader, writer);
    }

    static final class ZeroMQNodeStateDiffBuilder implements NodeStateDiff {

        private final Function<String, ZeroMQNodeState> reader;
        private final Consumer<String> writer;

        private Map<String, String> children;
        private Map<String, ZeroMQPropertyState> properties;

        private ZeroMQNodeStateDiffBuilder(ZeroMQNodeState before, Function<String, ZeroMQNodeState> reader, Consumer<String> writer) {
            this.reader = reader;
            this.writer = writer;
            reset();
            this.children.putAll(before.children);
            this.properties.putAll(before.properties);
        }

        private void reset() {
            this.children = new HashMap<>();
            this.properties = new HashMap<>();
        }

        public ZeroMQNodeState getNodeState() {
            final String uuid = UUID.randomUUID().toString();
            final ZeroMQNodeState ret = new ZeroMQNodeState(uuid, this.children, this.properties, reader, writer);
            ret.serialise(writer);
            return ret;
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            List<String> values = new ArrayList();
            if (after.isArray()) {
                for (int i = 0; i < after.count(); ++i) {
                    values.add(after.getValue(Type.STRING, i));
                }
            } else {
                try {
                    values.add(after.getValue(Type.STRING));
                } catch (ClassCastException e) {
                    log.error(e.toString());
                }
            }
            properties.put(after.getName(), new ZeroMQPropertyState(after.getName(), after.getType().toString(), values));
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
            return true;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            if (after instanceof ZeroMQNodeState) {
                this.children.put(name, ((ZeroMQNodeState) after).getUuid());
            } else {
                final ZeroMQNodeState before = (ZeroMQNodeState) ZeroMQEmptyNodeState.EMPTY_NODE(reader, writer);
                final ZeroMQNodeStateDiffBuilder diff = getNodeStateDiffBuilder(before, reader, writer);
                after.compareAgainstBaseState(before, diff);
                final ZeroMQNodeState child = diff.getNodeState();
                this.children.put(name, child.getUuid());
            }
            return true;
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            this.children.remove(name);
            return childNodeAdded(name, after);
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            this.children.remove(name);
            return true;
        }
    }
}
