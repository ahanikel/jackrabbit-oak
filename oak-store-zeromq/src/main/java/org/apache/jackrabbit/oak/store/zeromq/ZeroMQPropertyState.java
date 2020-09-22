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
import org.apache.jackrabbit.oak.plugins.memory.AbstractPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.plugins.value.Conversions.Converter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;

import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;

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

    private <T> T convertTo(String value, Type<T> type) {
        if (isStringBased(type)) {
            return (T) value;
        }

        if (type.equals(Type.BINARY)) {
            return (T) ns.getBlob(value);
        }

        Type<?> base = getType();
        if (base.isArray()) {
            base = base.getBaseType();
        }

        Converter converter = Conversions.convert(value, base);

        if (type == Type.BOOLEAN) {
            return (T) Boolean.valueOf(converter.toBoolean());
        } else if (type == Type.DECIMAL) {
            return (T) converter.toDecimal();
        } else if (type == Type.DOUBLE) {
            return (T) Double.valueOf(converter.toDouble());
        } else if (type == Type.LONG) {
            return (T) Long.valueOf(converter.toLong());
        } else {
            throw new UnsupportedOperationException(
                    "Unknown type: " + type);
        }
    }

    ZeroMQPropertyState(ZeroMQNodeStore ns, String name, String type,
                        List<String> values) {
        this.ns = ns;
        this.name = name;
        this.type = Type.fromString(type);
        this.stringValues = values;
        this.values = new ArrayList();
        values.forEach(v  ->
                this.values.add(convertTo(v, this.type.isArray()
                        ? this.type.getBaseType()
                        : this.type)));
    }

    <T> ZeroMQPropertyState(ZeroMQNodeStore ns, String name, Type<T> type, T value) {
        this.ns = ns;
        this.name = name;
        this.type = type;
        this.stringValues = new ArrayList();
        this.values = new ArrayList();
        if (type.isArray()) {
            ((Iterable<?>) value).forEach(this.values::add);
        } else {
            this.values.add(value);
        }
        this.values.forEach(v -> {
            final String sVal = valueToString(type.isArray() ? type.getBaseType() : type, v);
            this.stringValues.add(sVal);
        });
    }

    private ZeroMQPropertyState(ZeroMQNodeStore ns, PropertyState ps) {
        this.ns = ns;
        this.name = ps.getName();
        this.type = ps.getType();
        this.stringValues = new ArrayList<String>();
        this.values = new ArrayList<Object>();

        if (ps.isArray()) {
            for (int i = 0; i < ps.count(); ++i) {
                if (type.getBaseType()
                        .equals(Type.BINARY)) {
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
                    stringValues.add(ps.getValue(Type.STRING, i));
                    values.add(ps.getValue(type.getBaseType(), i));
                }
            }
        }
        else {
            if (type.equals(Type.BINARY)) {
                Blob blob = (Blob) ps.getValue(type);
                try {
                    if (!(blob instanceof ZeroMQBlobStoreBlob)) {
                        blob = ns.createBlob(blob); // ensure blob exists in the blobstore
                    }
                }
                catch (IOException ex) {
                    throw new IllegalStateException(ex);
                }
                stringValues.add(blob.getReference());
                values.add(blob);
            }
            else {
                stringValues.add(ps.getValue(Type.STRING));
                values.add(ps.getValue(type));
            }
        }
    }

    static ZeroMQPropertyState fromPropertyState(ZeroMQNodeStore ns, PropertyState p) {
        if (p instanceof ZeroMQPropertyState) {
            return (ZeroMQPropertyState) p;
        }
        return new ZeroMQPropertyState(ns, p);
    }

    static <T> List<String> fromValueToInternal(ZeroMQNodeStore ns, String name, Type<T> type, T value) {
        final List<String> ret = new ArrayList<>();
        switch (type.tag()) {
            case PropertyType.STRING:
            case PropertyType.NAME:
            case PropertyType.PATH:
            case PropertyType.REFERENCE:
            case PropertyType.DATE:
            case PropertyType.WEAKREFERENCE:
            case PropertyType.URI:
                if (type.isArray()) {
                    for (String v : (Iterable<String>) value) {
                        ret.add(v);
                    }
                } else {
                    ret.add((String) value);
                }
                break;
            case PropertyType.BINARY:
                if (value instanceof Blob) {
                    if (type.isArray()) {
                        for (Blob v : (Iterable<Blob>) value) {
                            try {
                                ret.add(ns.createBlob(v).getReference());
                            } catch (IOException e) {
                                throw new IllegalStateException(e);
                            }
                        }
                    } else {
                        try {
                            ret.add(ns.createBlob((Blob) value).getReference());
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    }
                } else if (value instanceof byte[]) {
                    if (type.isArray()) {
                        for (byte[] v : (Iterable<byte[]>) value) {
                            try {
                                ret.add(ns.createBlob(new ByteArrayInputStream(v)).getReference());
                            } catch (IOException e) {
                                throw new IllegalStateException(e);
                            }
                        }
                    } else {
                        try {
                            ret.add(ns.createBlob(new ByteArrayInputStream((byte[]) value)).getReference());
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    }
                }
                break;
            case PropertyType.LONG:
                if (type.isArray()) {
                    for (Long v : (Iterable<Long>) value) {
                        ret.add(v.toString());
                    }
                } else {
                    ret.add(((Long) value).toString());
                }
                break;
            case PropertyType.DOUBLE:
                if (type.isArray()) {
                    for (Double v : (Iterable<Double>) value) {
                        ret.add(v.toString());
                    }
                } else {
                    ret.add(((Double) value).toString());
                }
                break;
            case PropertyType.BOOLEAN:
                if (type.isArray()) {
                    for (Boolean v : (Iterable<Boolean>) value) {
                        ret.add(v.toString());
                    }
                } else {
                    ret.add(((Boolean) value).toString());
                }
                break;
            case PropertyType.DECIMAL:
                if (type.isArray()) {
                    for (BigDecimal v : (Iterable<BigDecimal>) value) {
                        ret.add(v.toString());
                    }
                } else {
                    ret.add(((BigDecimal) value).toString());
                }
                break;
            default:
                throw new IllegalArgumentException(value.getClass().toString());
        }
        return ret;
    }

    static ZeroMQPropertyState fromValue(ZeroMQNodeStore ns, String name, Type type, Object value) {
        final List<String> props = fromValueToInternal(ns, name, type, value);
        return new ZeroMQPropertyState(ns, name, type.toString(), props);
    }

    static ZeroMQPropertyState fromValue(ZeroMQNodeStore ns, String name, Object value) {
        if (value instanceof String) {
            final List<String> props = fromValueToInternal(ns, name, Type.STRING, (String) value);
            return new ZeroMQPropertyState(ns, name, Type.STRING.toString(), props);
        }
        if (value instanceof Blob) {
            final List<String> props = fromValueToInternal(ns, name, Type.BINARY, (Blob) value);
            return new ZeroMQPropertyState(ns, name, Type.BINARY.toString(), props);
        }
        if (value instanceof byte[]) {
            Blob blob;
            try {
                blob = ns.createBlob(new ByteArrayInputStream((byte[]) value));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            final List<String> props = fromValueToInternal(ns, name, Type.BINARY, blob);
            return new ZeroMQPropertyState(ns, name, Type.BINARY.toString(), props);
        }
        if (value instanceof Long) {
            final List<String> props = fromValueToInternal(ns, name, Type.LONG, (Long) value);
            return new ZeroMQPropertyState(ns, name, Type.LONG.toString(), props);
        }
        if (value instanceof Integer) {
            final List<String> props = fromValueToInternal(ns, name, Type.LONG, ((Integer) value).longValue());
            return new ZeroMQPropertyState(ns, name, Type.LONG.toString(), props);
        }
        if (value instanceof Double) {
            final List<String> props = fromValueToInternal(ns, name, Type.DOUBLE, (Double) value);
            return new ZeroMQPropertyState(ns, name, Type.DOUBLE.toString(), props);
        }
        if (value instanceof Boolean) {
            final List<String> props = fromValueToInternal(ns, name, BOOLEAN, (Boolean) value);
            return new ZeroMQPropertyState(ns, name, BOOLEAN.toString(), props);
        }
        if (value instanceof BigDecimal) {
            final List<String> props = fromValueToInternal(ns, name, Type.DECIMAL, (BigDecimal) value);
            return new ZeroMQPropertyState(ns, name, Type.DECIMAL.toString(), props);
        }
        if (value instanceof GregorianCalendar) {
            final List<String> props = fromValueToInternal(ns, name, Type.DATE, (((GregorianCalendar) value).toString()));
            return new ZeroMQPropertyState(ns, name, Type.DATE.toString(), props);
        }
        else throw new IllegalArgumentException(value.getClass().toString());
    }

    @Override
    public @NotNull String getName() {
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
        return type.equals(Type.STRING) || type.equals(Type.DATE) || type.equals(Type.NAME)
                       || type.equals(Type.PATH) || type.equals(Type.REFERENCE) || type
                .equals(
                        Type.WEAKREFERENCE) || type.equals(Type.URI);
    }

    private static boolean areStringBased(Type<?> type) {
        return type.equals(Type.STRINGS) || type.equals(Type.DATES) || type.equals(Type.NAMES)
                       || type.equals(Type.PATHS) || type.equals(Type.REFERENCES) || type
                .equals(
                        Type.WEAKREFERENCES) || type.equals(Type.URIS);
    }

    @Override
    public <T> @NotNull T getValue(Type<T> type) {
        if (type.isArray()) {
            if (this.type.equals(type)) {
                return (T) Collections.unmodifiableList(values);
            }
            else if (type.equals(Type.STRINGS)) {
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
            else if (type.equals(Type.STRING)) {
                return (T) stringValues.get(0);
            }
            else {
                return (T) convertTo(stringValues.get(0), type);
            }
        }
    }

    @Override
    public <T> @NotNull T getValue(Type<T> type, int index) {
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
        else if (type.equals(Type.STRING)) {
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
        if (that instanceof ZeroMQPropertyState) {
            ZeroMQPropertyState other = (ZeroMQPropertyState) that;
            if (this.getType().equals(Type.BINARY) && other.getType().equals(Type.BINARY)) {
                return this.stringValues.get(0).equals(other.stringValues.get(0));
            }
        }
        if (that instanceof BinaryPropertyState) {
            BinaryPropertyState other = (BinaryPropertyState) that;
            if (this.getType().equals(Type.BINARY) && other.getType().equals(Type.BINARY)) {
                return this.stringValues.get(0).equals(other.getValue().getReference());
            }
        }
        if (!(that instanceof PropertyState)) {
            return false;
        }
        return AbstractPropertyState.equal(this, (PropertyState) that);
    }

    @Override
    public int hashCode() {
        return AbstractPropertyState.hashCode(this);
    }

    public StringBuilder serialise(final StringBuilder sb) {
        sb.append(safeEncode(getName()));
        sb.append(" <");
        sb.append(getType());
        sb.append("> ");
        if (isArray()) {
            sb.append("= [");
            stringValues.forEach((String s)  -> {
                if (type.equals(Type.BINARIES)) {
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
                if (type.equals(Type.BINARY)) {
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

    private static String valueToString(Type<?> from, Object v) {
        if (from.equals(Type.STRING) || from.equals(Type.DATE) || from.equals(Type.NAME)
                    || from.equals(Type.PATH) || from.equals(Type.REFERENCE) || from
                .equals(
                        Type.WEAKREFERENCE) || from.equals(Type.URI)) {
            return (String) v;
        }
        else if (from.equals(Type.BINARY)) {
            return ((Blob) v).getReference();
        }
        else if (from.equals(Type.LONG) || from.equals(Type.DOUBLE) || from
                .equals(BOOLEAN) || from.equals(Type.DECIMAL)) {
            return v.toString();
        }
        else {
            throw new IllegalArgumentException(from.toString());
        }
    }
}