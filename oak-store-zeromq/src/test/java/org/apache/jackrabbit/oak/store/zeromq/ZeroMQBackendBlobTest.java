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

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ZeroMQBackendBlobTest {

    private ZeroMQFixture fixture;
    private ZeroMQNodeStore ns;

    @Before
    public void setup() {
        fixture = new ZeroMQFixture();
        ns = (ZeroMQNodeStore) fixture.createNodeStore();
    }

    @After
    public void dispose() {
        fixture.dispose(ns);
    }

    @Test
    public void testBlobBackend() throws IOException {
        int[] sizes = new int[]{
                //1024 * 1024 * 201 + 17,
                //1024 * 1024 * 101,
                //1024 * 1024 * 100,
                //1024 * 1024 * 99,
                1024 * 1024 * 10,
        };
        Blob[] blobWritten = new Blob[sizes.length];
        Blob[] blobRead    = new Blob[sizes.length];
        for (int i = 0; i < sizes.length; ++i) {
            blobWritten[i] = ns.createBlob(new ByteInputStreamGenerator(sizes[i]));
        }
        ns.blobCache.invalidateAll();
        assertTrue(new File("/tmp/blobs", "8E53463838ADC859873BBB1A172E1AB1").delete());
        for (int i = 0; i < sizes.length; ++i) {
            blobRead[i] = ns.getBlob(blobWritten[i].getReference());
            assertEquals(blobRead[i].getReference(), blobWritten[i].getReference());
        }
        for (int i = 0; i < sizes.length; ++i) {
            InputStream is = blobRead[i].getNewStream();
            int offset = 0;
            int b = is.read();
            for (; b >= 0; b = is.read(), ++offset) {
                assertEquals("Offset " + offset, offset & 0xff, b);
            }
            assertEquals(offset, sizes[i]);
        }
        FileUtils.deleteDirectory(new File("/tmp/blobs"));
    }

    static class ByteInputStreamGenerator extends InputStream {
        private final int max;
        private int count;

        public ByteInputStreamGenerator(int max) {
            this.max = max;
            this.count = 0;
        }

        @Override
        public int read() {
            if (count < max) {
                return count++ & 0xff;
            }
            return -1;
        }

        @Override
        public int available() {
            return max - count;
        }
    }
}
