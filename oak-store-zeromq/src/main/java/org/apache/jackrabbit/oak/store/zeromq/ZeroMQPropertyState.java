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

import java.io.IOException;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.jackrabbit.oak.api.Type.*;
import org.apache.jackrabbit.oak.plugins.memory.AbstractPropertyState;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.Conversions.Converter;

public class ZeroMQPropertyState implements PropertyState {

    private static final Logger log = LoggerFactory.getLogger(
            ZeroMQPropertyState.class);

    private static final DateFormat dateParser = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    private final ZeroMQNodeStore ns;

    private final String name;

    private final Type type;

    private final List<String> stringValues;

    private final List<Object> values;

    private Object convertTo(String value, Type type) {
        if (isStringBased(type)) {
            return value;
        }

        if (type.equals(BINARY)) {
            return ns.getBlob(value);
        }

        Converter conv = Conversions.convert(value);

        if (type.equals(BOOLEAN)) {
            return conv.toBoolean();
        }

        if (type.equals(DECIMAL)) {
            return conv.toDecimal();
        }

        if (type.equals(DOUBLE)) {
            return conv.toDouble();
        }

        if (type.equals(LONG)) {
            return conv.toLong();
        }

        throw new IllegalArgumentException("Unknown type: " + type.toString());
    }

    ZeroMQPropertyState(ZeroMQNodeStore ns, String name, String type,
                        List<String> values) {
        this.ns = ns;
        this.name = name;
        this.type = Type.fromString(type);
        this.stringValues = values;
        this.values = new ArrayList();
        values.stream()
                .forEach(v  -> this.values.add(convertTo(v, this.type.isArray()
                                                                     ? this.type
                                                                  .getBaseType()
                                                                     : this.type)));
    }

    public ZeroMQPropertyState(ZeroMQNodeStore ns, PropertyState ps) {
        this.ns = ns;
        this.name = ps.getName();
        this.type = ps.getType();
        this.stringValues = new ArrayList();
        this.values = new ArrayList();

        if (ps.isArray()) {
            for (int i = 0; i < ps.count(); ++i) {
                if (type.getBaseType()
                        .equals(BINARY)) {
                    Blob blob = (Blob) ps.getValue(type.getBaseType(), i);
                    try {
                        blob = ns.createBlob(blob); // ensure blob exists in the blobstore
                    }
                    catch (IOException ex) {
                        throw new IllegalStateException(ex);
                    }
                    stringValues.add(blob.getReference());
                    values.add(blob);
                }
                else {
                    stringValues.add(ps.getValue(STRING, i));
                    values.add(ps.getValue(type.getBaseType(), i));
                }
            }
        }
        else {
            if (type.equals(BINARY)) {
                Blob blob = (Blob) ps.getValue(type);
                try {
                    blob = ns.createBlob(blob); // ensure blob exists in the blobstore
                }
                catch (IOException ex) {
                    throw new IllegalStateException(ex);
                }
                stringValues.add(blob.getReference());
                values.add(blob);
            }
            else {
                stringValues.add(ps.getValue(STRING));
                values.add(ps.getValue(type));
            }
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

    private static boolean isStringBased(Type<?> type) {
        return type.equals(STRING) || type.equals(DATE) || type.equals(NAME)
                       || type.equals(PATH) || type.equals(REFERENCE) || type
                .equals(
                        WEAKREFERENCE) || type.equals(URI);
    }

    private static boolean areStringBased(Type<?> type) {
        return type.equals(STRINGS) || type.equals(DATES) || type.equals(NAMES)
                       || type.equals(PATHS) || type.equals(REFERENCES) || type
                .equals(
                        WEAKREFERENCES) || type.equals(URIS);
    }

    @Override
    public <T> T getValue(Type<T> type) {
        if (type.isArray()) {
            if (this.type.equals(type)) {
                return (T) Collections.unmodifiableList(values);
            }
            else if (type.equals(STRINGS)) {
                return (T) Collections.unmodifiableList(stringValues);
            }
            else {
                List ret = new ArrayList();
                stringValues.stream()
                        .forEach(v  -> ret
                                .add(convertTo(v, type.getBaseType())));
                return (T) ret;
            }
        }
        else {
            if (this.type.equals(type)) {
                return (T) values.get(0);
            }
            else if (type.equals(STRING)) {
                return (T) stringValues.get(0);
            }
            else {
                return (T) convertTo(stringValues.get(0), type);
            }
        }
    }

    @Override
    public <T> T getValue(Type<T> type, int index) {
        if (index < 0 || index >= this.count()) {
            throw new IndexOutOfBoundsException(String.format(
                    "index %d requested but we only have %d values", index,
                    count()));
        }

        if (type.isArray() || type.equals(Type.UNDEFINED)) {
            throw new IllegalArgumentException(type.toString());
        }

        if (this.type.equals(type)) {
            return (T) values.get(index);
        }
        else if (type.equals(STRING)) {
            return (T) stringValues.get(index);
        }
        else {
            return (T) convertTo(stringValues.get(index), type);
        }
    }

    @Override
    public long size() {
        if (this.isArray()) {
            throw new IllegalStateException();
        }
        return stringValues.get(0)
                .length();
    }

    @Override
    public long size(int index) {
        if (index < 0 || index >= this.count()) {
            throw new IndexOutOfBoundsException(String.format(
                    "index %d requested but we only have %d values", index,
                    count()));
        }
        return stringValues.get(index)
                .length();
    }

    @Override
    public int count() {
        if (type.isArray()) {
            return values.size();
        }
        else {
            return 1;
        }
    }

    @Override
    public boolean equals(Object that) {
        if (!(that instanceof PropertyState)) {
            return false;
        }
        PropertyState other = (PropertyState) that;
        return this.getType()
                .equals(other.getType()) && this.getName()
                .equals(other.getName()) && this.getValue(this.getType())
                .equals(other.getValue(other.getType()));
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 11 * hash + Objects.hashCode(this.name);
        hash = 11 * hash + Objects.hashCode(this.type);
        hash = 11 * hash + Objects.hashCode(this.stringValues);
        return hash;
    }

    public StringBuilder serialise(final StringBuilder sb) {
        sb.append(safeEncode(getName()));
        sb.append(" <");
        sb.append(getType());
        sb.append("> ");
        if (isArray()) {
            sb.append("= [");
            stringValues.forEach((String s)  -> {
                if (type.equals(BINARIES)) {
                    sb.append(s);
                }
                else {
                    sb.append(safeEncode(s));
                }
                sb.append(',');
            });
            replaceOrAppendLastChar(sb, ',', ']');
        }
        else {
            try {
                sb.append("= ");
                if (type.equals(BINARY)) {
                    sb.append(stringValues.get(0));
                }
                else {
                    sb.append(safeEncode(stringValues.get(0)));
                }
            }
            catch (ClassCastException e) {
                log.error(e.toString());
            }
        }
        return sb;
    }

    private static void replaceOrAppendLastChar(StringBuilder b, char oldChar,
                                                char newChar) {
        if (b.charAt(b.length() - 1) == oldChar) {
            b.setCharAt(b.length() - 1, newChar);
        }
        else {
            b.append(newChar);
        }
    }

    private static String safeEncode(String value) {
        try {
            return SafeEncode.safeEncode(value);
        }
        catch (UnsupportedEncodingException e) {
            return "ERROR: " + e;
        }
    }

    private static <T> String valueToString(Type<T> from, T v) {
        if (from.equals(STRING) || from.equals(DATE) || from.equals(NAME)
                    || from.equals(PATH) || from.equals(REFERENCE) || from
                .equals(
                        WEAKREFERENCE) || from.equals(URI)) {
            return (String) v;
        }
        else if (from.equals(BINARY)) {
            return ((Blob) v).getReference();
        }
        else if (from.equals(LONG) || from.equals(DOUBLE) || from
                .equals(BOOLEAN) || from.equals(DECIMAL)) {
            return v.toString();
        }
        else {
            throw new IllegalArgumentException(from.toString());
        }
    }
}
