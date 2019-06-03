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

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.util.SafeEncode;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.apache.jackrabbit.oak.api.Type.*;

public class ZeroMQPropertyState implements PropertyState {

    private final String name;
    private final Type type;
    private final Object value;

    public ZeroMQPropertyState(String name, String type, List<String> values) {
        this.name = name;
        this.type = Type.fromString(type);
        if (
                this.type.equals(STRING) ||
                this.type.equals(DATE) ||
                this.type.equals(NAME) ||
                this.type.equals(PATH) ||
                this.type.equals(REFERENCE) ||
                this.type.equals(WEAKREFERENCE) ||
                this.type.equals(URI)
        ) {
            this.value = values.get(0);
        } else if (
                this.type.equals(STRINGS) ||
                this.type.equals(DATES) ||
                this.type.equals(NAMES) ||
                this.type.equals(PATHS) ||
                this.type.equals(REFERENCES) ||
                this.type.equals(WEAKREFERENCES) ||
                this.type.equals(URIS)
        ) {
            this.value = values;
        } else if (this.type.equals(BINARY)) {
            this.value = values;
        } else if (this.type.equals(BINARIES)) {
            this.value = values;
        } else if (this.type.equals(LONG)) {
            this.value = Long.valueOf(values.get(0));
        } else if (this.type.equals(LONGS)) {
            this.value = new ArrayList<Long>();
            values.stream().map(Long::valueOf).forEach(((ArrayList<Long>) this.value)::add);
        } else if (this.type.equals(DOUBLE)) {
            this.value = Double.valueOf(values.get(0));
        } else if (this.type.equals(DOUBLES)) {
            this.value = new ArrayList<Double>();
            values.stream().map(Double::valueOf).forEach(((ArrayList<Double>) this.value)::add);
        } else if (this.type.equals(BOOLEAN)) {
            this.value = Boolean.valueOf(values.get(0));
        } else if (this.type.equals(BOOLEANS)) {
            this.value = new ArrayList<Boolean>();
            values.stream().map(Boolean::valueOf).forEach(((ArrayList<Boolean>) this.value)::add);
        } else if (this.type.equals(DECIMAL)) {
            this.value = new BigDecimal(values.get(0));
        } else if (this.type.equals(DECIMALS)) {
            this.value = new ArrayList<BigDecimal>();
            values.stream().map(v -> new BigDecimal(v)).forEach(((ArrayList<BigDecimal>) this.value)::add);
        } else {
            throw new IllegalArgumentException(type);
        }
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
        return (T) value;
    }

    @Override
    public <T> T getValue(Type<T> type, int index) {
        if (type.isArray()) {
            throw new IllegalArgumentException(type.toString());
        }
        return ((ArrayList<T>) this.value).get(index); // TODO: see doc
    }

    @Override
    public long size() {
        // TODO: not sure if that's correct
        if (type.isArray()) {
            return ((ArrayList) value).size();
        } else {
            return 1;
        }
    }

    @Override
    public long size(int index) {
        // TODO:
        return 1;
    }

    @Override
    public int count() {
        // TODO: not sure if that's correct
        if (type.isArray()) {
            return ((ArrayList) value).size();
        } else {
            return 1;
        }
    }

    public StringBuilder serialise(final StringBuilder sb) {
        sb.append(safeEncode(getName()));
        sb.append(" <");
        sb.append(getType());
        sb.append("> ");
        if (getType() == BINARY) {
            sb.append("= ");
            final Blob blob = getValue(BINARY);
            appendBlob(sb, blob);
        } else if (getType() == BINARIES) {
            sb.append("= [");
            getValue(BINARIES).forEach((Blob b) -> {
                appendBlob(sb, b);
                sb.append(',');
            });
            replaceOrAppendLastChar(sb, ',', ']');
        } else if (isArray()) {
            sb.append("= [");
            getValue(STRINGS).forEach((String s) -> {
                sb.append(safeEncode(s));
                sb.append(',');
            });
            replaceOrAppendLastChar(sb, ',', ']');
        } else {
            sb.append("= ").append(safeEncode(getValue(STRING)));
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
