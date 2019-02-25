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

import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;

public class DistributedSegmentMap implements ConcurrentMap<SegmentId, Segment> {

    ConcurrentMap<SegmentId, Segment> localSegments = Maps.newConcurrentMap();
    ConcurrentMap<SegmentId, Segment> remoteSegments = null;

    private ConcurrentMap<SegmentId, Segment> segments(SegmentId id) {
        if (isOurs(id)) {
            return localSegments;
        }
        else {
            return remoteSegments;
        }
    }

    private boolean isOurs(SegmentId id) {
        return true;
    }

    @Override
    public Segment putIfAbsent(SegmentId key, Segment value) {
        return segments(key).putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return segments((SegmentId) key).remove(key, value);
    }

    @Override
    public boolean replace(SegmentId key, Segment oldValue, Segment newValue) {
        return segments(key).replace(key, oldValue, newValue);
    }

    @Override
    public Segment replace(SegmentId key, Segment value) {
        return segments(key).replace(key, value);
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean containsKey(Object key) {
        return segments((SegmentId) key).containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Segment get(Object key) {
        return segments((SegmentId) key).get(key);
    }

    @Override
    public Segment put(SegmentId key, Segment value) {
        return segments(key).put(key, value);
    }

    @Override
    public Segment remove(Object key) {
        return segments((SegmentId) key).remove(key);
    }

    @Override
    public void putAll(Map<? extends SegmentId, ? extends Segment> m) {
        m.forEach(this::put);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<SegmentId> keySet() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Collection<Segment> values() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<Entry<SegmentId, Segment>> entrySet() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
