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

import com.google.common.primitives.Longs;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ZeroMQNodeState extends AbstractNodeState {

    private static final Logger log = LoggerFactory.getLogger(ZeroMQNodeState.class);

    private static final Pattern endChildrenPattern = Pattern.compile("(end children\\n).*", Pattern.DOTALL);
    private static final Pattern endPropertiesPattern = Pattern.compile("(end properties\\n).*", Pattern.DOTALL);
    private static final Pattern tabSeparatedPattern = Pattern.compile("([^\\t]+\\t).*", Pattern.DOTALL);
    private static final Pattern spaceSeparatedPattern = Pattern.compile("([^ ]+ ).*", Pattern.DOTALL);
    private static final Pattern propertyTypePattern = Pattern.compile("([^>]+> = ).*", Pattern.DOTALL);
    private static final Pattern allTheRestPattern = Pattern.compile("(.*)", Pattern.DOTALL);

    private static MessageDigest md;

    static {
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            md = null;
        }
    }

    protected final ZeroMQNodeStore ns;
    private final String uuid;
    final Map<String, String> children;
    final Map<String, ZeroMQPropertyState> properties;
    private final Function<String, ZeroMQNodeState> reader;
    private final Consumer<ZeroMQNodeState> writer;
    private final String serialised;
    private List<ChildNodeEntry> childNodeEntries;

    // not private because ZeroMQEmptyNodeState needs it
    ZeroMQNodeState(ZeroMQNodeStore ns, Function<String, ZeroMQNodeState> reader, Consumer<ZeroMQNodeState> writer) {
        this.ns = ns;
        this.children = new HashMap<>(1000);
        this.properties = new HashMap<>(100);
        this.reader = reader;
        this.writer = writer;
        this.uuid = ZeroMQEmptyNodeState.UUID_NULL.toString();
        serialised = serialise();
    }

    ZeroMQNodeState(ZeroMQNodeStore ns, Map<String, String> children, Map<String, ZeroMQPropertyState> properties, String serialised, Function<String, ZeroMQNodeState> reader, Consumer<ZeroMQNodeState> writer) {
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

    static ZeroMQNodeState deSerialise(ZeroMQNodeStore ns, String s, Function<String, ZeroMQNodeState> reader, Consumer<ZeroMQNodeState> writer) throws ParseFailure {
        final List<String> children = new ArrayList<>();
        final List<String> properties = new ArrayList<>();
        final Map<String, String> childrenMap = new HashMap<>(1000);
        final Map<String, ZeroMQPropertyState> propertiesMap = new HashMap<>(100);
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

    public String getSerialised() {
        return serialised;
    }

    private String serialise() {
        final AtomicReference<Exception> e = new AtomicReference<>();
        final StringBuilder sb = new StringBuilder();
        sb
            .append("begin ZeroMQNodeState\n")
            .append("begin children\n");
        List<String> childNames = new ArrayList<>(children.keySet());
        childNames.sort(Comparator.naturalOrder());
        childNames.forEach(name ->
        {
            try {
                sb
                    .append(SafeEncode.safeEncode(name))
                    .append('\t')
                    .append(children.get(name))
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
        List<String> propertyNames = new ArrayList<>(properties.keySet());
        propertyNames.sort(Comparator.naturalOrder());
        propertyNames.forEach(name ->
            properties.get(name).serialise(sb).append('\n')
        );
        sb.append("end properties\n");
        sb.append("end ZeroMQNodeState\n");
        return sb.toString();
    }

    private String generateUuid() throws UnsupportedEncodingException {
        synchronized (md) {
            if (children.isEmpty() && properties.isEmpty()) {
                return ZeroMQEmptyNodeState.UUID_NULL.toString();
            }
            final byte[] digest = md.digest(serialised.getBytes("UTF-8"));
            final long msb = Longs.fromByteArray(Arrays.copyOfRange(digest, 0, 8));
            final long lsb = Longs.fromByteArray(Arrays.copyOfRange(digest, 8, 16));
            return new UUID(msb, lsb).toString();
        }
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
            return ns.missingNode;
        }
    }

    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        if (childNodeEntries == null) {
            childNodeEntries =
                    children.entrySet().stream().map(child -> new ChildNodeEntry() {
                        @Override
                        public String getName() {
                            return child.getKey();
                        }

                        @Override
                        public NodeState getNodeState() {
                            return ZeroMQNodeState.this.getChildNode(child.getKey());
                        }
                    }).collect(Collectors.toList());
        }
        return (() -> childNodeEntries.iterator());
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
}
