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
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import java.util.ArrayList;
import java.util.List;

public class MergingApplyDiff extends ApplyDiff {

    public MergingApplyDiff(NodeBuilder builder) {
        super(builder);
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        if (after.isArray() && builder.getProperty(after.getName()).getType().equals(after.getType())) {
            mergeArray(null, (SimplePropertyState) after);
        } else {
            super.propertyAdded(after);
        }
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        if (after.isArray() && builder.getProperty(after.getName()).getType().equals(after.getType())) {
            mergeArray((SimplePropertyState) before, (SimplePropertyState) after);
        } else {
            super.propertyChanged(before, after);
        }
        return true;
    }

    void mergeArray(SimplePropertyState before, SimplePropertyState after) {
        final List<Object> targetObjects = (List<Object>) after.getValue(after.getType());
        final List<Object> mergedObjects = new ArrayList<>();
        mergedObjects.addAll((List<Object>) builder.getProperty(after.getName()).getValue(after.getType()));
        for (Object o : (List<Object>) after.getValue(after.getType())) {
            if (!mergedObjects.contains(o)) {
                mergedObjects.add(o);
            }
        }
        if (before != null) {
            for (Object o : (List<Object>) before.getValue(after.getType())) {
                if (!targetObjects.contains(o)) {
                    mergedObjects.remove(o);
                }
            }
        }
        final SimplePropertyState merged = SimplePropertyState.fromValue(after.getStore(), after.getName(), after.getType(), mergedObjects);
        builder.setProperty(merged);
    }
}
