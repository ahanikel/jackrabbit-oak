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
import org.apache.jackrabbit.oak.plugins.memory.StringBasedBlob;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.List;

import static org.apache.jackrabbit.oak.api.Type.*;

public class ZeroMQPropertyState implements PropertyState {

    private static final Logger log = LoggerFactory.getLogger(ZeroMQPropertyState.class);

    private final String name;
    private final Type type;
    private final List<String> values;

    public ZeroMQPropertyState(String name, String type, List<String> values) {
        this.name = name;
        this.type = Type.fromString(type);
        this.values = values;
    }

    public ZeroMQPropertyState(PropertyState ps) {
        this.values = new ArrayList<>();
        this.name = ps.getName();
        this.type = ps.getType();
        if (ps.isArray()) {
            for (int i = 0; i < ps.count(); ++i) {
                this.values.add(valueToString(type.getBaseType(), ps.getValue(type.getBaseType(), i)));
            }
        }
        this.values.add(valueToString(type, ps.getValue(type)));
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isArray() {
        return type.isArray();
    }

    @Override
    public Type<?> getType() {
        return type;
    }

    @Override
    public <T> T getValue(Type<T> type) {
        if (type.isArray()) {
            if (
                    type.equals(STRINGS) ||
                    type.equals(DATES) ||
                    type.equals(NAMES) ||
                    type.equals(PATHS) ||
                    type.equals(REFERENCES) ||
                    type.equals(WEAKREFERENCES) ||
                    type.equals(URIS)
            ) {
                return (T) values;
            } else {
                throw new IllegalStateException(type.toString());
            }
        } else {
            return getValue(type, 0);
        }
    }

    @Override
    public <T> T getValue(Type<T> type, int index) {
        if (
                type.equals(STRING) ||
                type.equals(DATE) ||
                type.equals(NAME) ||
                type.equals(PATH) ||
                type.equals(REFERENCE) ||
                type.equals(WEAKREFERENCE) ||
                type.equals(URI)
        ) {
            return (T) values.get(index);
        } else if (type.equals(BINARY)) {
            return (T) blobFromString(values.get(index));
        } else if (type.equals(LONG)) {
            return (T) Long.valueOf(values.get(index));
        } else if (type.equals(DOUBLE)) {
            return (T) Double.valueOf(values.get(index));
        } else if (type.equals(BOOLEAN)) {
            return (T) Boolean.valueOf(values.get(index));
        } else if (type.equals(DECIMAL)) {
            return (T) new BigDecimal(values.get(index));
        } else {
            throw new IllegalArgumentException(type.toString());
        }
    }

    @Override
    public long size() {
        // TODO: not sure if that's correct
        return values.get(0).length();
    }

    @Override
    public long size(int index) {
        return values.get(index).length();
    }

    @Override
    public int count() {
        // TODO: not sure if that's correct
        if (type.isArray()) {
            return values.size();
        } else {
            return 1;
        }
    }

    public StringBuilder serialise(final StringBuilder sb) {
        sb.append(safeEncode(getName()));
        sb.append(" <");
        sb.append(getType());
        sb.append("> ");
        if (isArray()) {
            sb.append("= [");
            values.forEach((String s) -> {
                if (type.equals(BINARIES)) {
                    sb.append(s);
                } else {
                    sb.append(safeEncode(s));
                }
                sb.append(',');
            });
            replaceOrAppendLastChar(sb, ',', ']');
        } else {
            try {
                sb.append("= ");
                if (type.equals(BINARY)) {
                    sb.append(values.get(0));
                } else {
                    sb.append(safeEncode(values.get(0)));
                }
            } catch (ClassCastException e) {
                log.error(e.toString());
            }
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

    private static Blob blobFromString(String s) {
        return new Blob() {

            @Override
            public @NotNull InputStream getNewStream() {
                return new InputStream() {

                    StringCharacterIterator it = new StringCharacterIterator(s);

                    private int hexCharToInt(char c) {
                        return Character.isDigit(c) ? c - '0' : c - 'A' + 10;
                    }

                    @Override
                    public int read() throws IOException {
                        final int c = hexCharToInt(it.next());
                        final int d = hexCharToInt(it.next());
                        final int ret = c << 4 | d;
                        return ret;
                    }
                };
            }

            @Override
            public long length() {
                return s.length() / 2;
            }

            @Override
            public @Nullable String getReference() {
                return null;
            }

            @Override
            public @Nullable String getContentIdentity() {
                return null;
            }
        };
    }

    private static String safeEncode(String value) {
        try {
            return SafeEncode.safeEncode(value);
        } catch (UnsupportedEncodingException e) {
            return "ERROR: " + e;
        }
    }

    private static <T> String valueToString(Type<T> from, T v) {
        if (
                from.equals(STRING) ||
                from.equals(DATE) ||
                from.equals(NAME) ||
                from.equals(PATH) ||
                from.equals(REFERENCE) ||
                from.equals(WEAKREFERENCE) ||
                from.equals(URI)
        ) {
            return (String) v;
        } else if (from.equals(BINARY)) {
            final StringBuilder sb = new StringBuilder();
            appendBlob(sb, (Blob) v);
            return sb.toString();
        } else if (
                from.equals(LONG) ||
                from.equals(DOUBLE) ||
                from.equals(BOOLEAN) ||
                from.equals(DECIMAL)
        ) {
            return v.toString();
        } else {
            throw new IllegalArgumentException(from.toString());
        }
    }
}
