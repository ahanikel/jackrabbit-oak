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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;

import java.util.List;

public class ZeroMQPropertyState implements PropertyState {

    private final String name;
    private final Type type;
    private final List<String> values;

    public ZeroMQPropertyState(String name, String type, List<String> values) {
        this.name = name;
        this.type = Type.fromString(type);
        this.values = values;
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
        if (type.equals(Type.STRING)) {
            return (T) values.get(0);
        } else if (type.equals(Type.STRINGS)) {
            return (T) values;
        } // TODO: implement rest
        return null;
    }

    @Override
    public <T> T getValue(Type<T> type, int index) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long size() {
        return values.size();
    }

    @Override
    public long size(int index) {
        // TODO:
        return values.get(index).length();
    }

    @Override
    public int count() {
        return values.size();
    }
}
