package org.apache.jackrabbit.oak.segment.memory;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.CachingSegmentReader;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.NoopStats;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.junit.Assert.assertEquals;

public class MemoryStoreTest {

    @Test
    public void createSegmentBlobTest() throws Exception {
        final MemoryStore store = new MemoryStore();
        final NodeBuilder builder = EMPTY_NODE.builder();

        final InputStream testData = new InputStream() {
            private int state = 0;

            @Override
            public int read() throws IOException {
                if (state > 30_000_000) {
                    return -1;
                }
                return ++state % 256;
            }
        };

        final Blob blob = builder.createBlob(testData);
        builder.child("root").setProperty("testData", blob);
        final NodeState nodeState = builder.getNodeState();

        final SegmentWriter writer = defaultSegmentWriterBuilder("sys").withWriterPool().build(store);
        final RecordId head = writer.writeNode(nodeState);
        writer.flush();

        final SegmentReader reader = new CachingSegmentReader(() -> writer, null, 16, 2, NoopStats.INSTANCE);
        final Blob blobRead = reader.readNode(head).getChildNode("root").getProperty("testData").getValue(Type.BINARY);
        final InputStream is = blobRead.getNewStream();
        for (int i = 1, intRead = is.read(); intRead >= 0; i = ++i % 256, intRead = is.read()) {
            assertEquals(i, intRead);
        }
    }
}
