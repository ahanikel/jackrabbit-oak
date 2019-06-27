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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.apache.jackrabbit.oak.api.Type.*;

public class ZeroMQPropertyState implements PropertyState {

    private static final Logger log = LoggerFactory.getLogger(ZeroMQPropertyState.class);

    private final ZeroMQNodeStore ns;
    private final String name;
    private final Type type;
    private final List<String> values;

    ZeroMQPropertyState(ZeroMQNodeStore ns, String name, String type, List<String> values) {
        this.ns = ns;
        this.name = name;
        this.type = Type.fromString(type);
        this.values = values;
    }

    public ZeroMQPropertyState(ZeroMQNodeStore ns, PropertyState ps) {
        this.values = new ArrayList<>();
        this.ns = ns;
        this.name = ps.getName();
        this.type = ps.getType();
        if (ps.isArray()) {
            for (int i = 0; i < ps.count(); ++i) {
                this.values.add(valueToString(type.getBaseType(), ps.getValue(type.getBaseType(), i)));
            }
        } else {
            this.values.add(valueToString(type, ps.getValue(type)));
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
        if (type.isArray()) {
            if (
                    type.equals(STRINGS) ||
                    type.equals(DATES) ||
                    type.equals(NAMES) ||
                    type.equals(PATHS) ||
                    type.equals(REFERENCES) ||
                    type.equals(WEAKREFERENCES) ||
                    type.equals(URIS) ||
                    type.equals(this.type)
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
            return (T) ZeroMQBlob.newInstance(this.ns, values.get(index));
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
        if (count() == 0) {
            return 0;
        }
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

    @Override
    public boolean equals(Object that) {
        if (!(that instanceof PropertyState)) {
            return false;
        }
        PropertyState other = (PropertyState) that;
        return
            this.getType().equals(other.getType()) &&
            this.getName().equals(other.getName()) &&
            this.getValue(this.getType()).equals(other.getValue(other.getType()))
        ;
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
            return ((Blob) v).getReference();
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
