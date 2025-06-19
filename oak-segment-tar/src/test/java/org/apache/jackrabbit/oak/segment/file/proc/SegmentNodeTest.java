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

package org.apache.jackrabbit.oak.segment.file.proc;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Optional;

import com.codahale.metrics.MetricSet;
import org.apache.jackrabbit.oak.segment.CachingSegmentReader;
import org.apache.jackrabbit.oak.segment.MapRecord;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentIdFactory;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.Template;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.MetricsIOMonitor;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend.Segment;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.bouncycastle.util.encoders.HexEncoder;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class SegmentNodeTest {

    @Test
    public void shouldHandleMissingSegment() {
        Backend backend = mock(Backend.class);
        when(backend.getSegment("s")).thenReturn(Optional.empty());

        assertEquals(MissingSegmentNode.class, SegmentNode.newSegmentNode(backend, "s").getClass());
    }

    @Test
    public void shouldHandleDataSegment() {
        Segment segment = mock(Segment.class);
        when(segment.isDataSegment()).thenReturn(true);

        Backend backend = mock(Backend.class);
        when(backend.getSegment("s")).thenReturn(Optional.of(segment));

        assertEquals(DataSegmentNode.class, SegmentNode.newSegmentNode(backend, "s").getClass());
    }

    @Test
    public void shouldHandleBulkSegment() {
        Segment segment = mock(Segment.class);
        when(segment.isDataSegment()).thenReturn(false);

        Backend backend = mock(Backend.class);
        when(backend.getSegment("s")).thenReturn(Optional.of(segment));

        assertEquals(BulkSegmentNode.class, SegmentNode.newSegmentNode(backend, "s").getClass());
    }

    @Test
    public void bla() throws InvalidFileStoreVersionException, IOException {
        FileStore fs = FileStoreBuilder
                .fileStoreBuilder(new File("/Users/axel/Downloads/org.apache.sling.feature.launcher-1.3.0/launcher/repository/segmentstore"))
                .build();
        SegmentId segmentId = new SegmentId(fs, 4909755921926278383L, -6505580142685085918L);
        org.apache.jackrabbit.oak.segment.Segment segment = fs.readSegment(segmentId);
        CachingSegmentReader reader = new CachingSegmentReader(() -> null, null, 0, 0, new MeterStats() {
            @Override
            public long getCount() {
                return 0;
            }

            @Override
            public void mark() {
            }

            @Override
            public void mark(long l) {
            }
        });
        int offset = segment.getOffset(41);
        System.out.println("Offset is: " + offset);
        SegmentNodeState segmentNodeState = reader.readNode(new RecordId(segmentId, 41));
        System.out.println("Template id: " + segmentNodeState.getTemplateId().getRecordNumber());
        System.out.println("Template offset: " + segment.getOffset(segmentNodeState.getTemplateId().getRecordNumber()));
        Template template = segmentNodeState.getTemplate();
        System.out.println("Template is: " + template);
        MapRecord childNodeMap = template.getChildNodeMap(segmentNodeState.getRecordId());
        System.out.println("ChildNodeMap is: " + childNodeMap);
        System.out.println("ChildNodeMap id: " + childNodeMap.getRecordId() + "/" + childNodeMap.getRecordId().getRecordNumber());
        //MapRecord childNodeMap = segmentNodeState.getChildNodeMap();
        //System.out.println("Map is: " + childNodeMap);
    }
}
