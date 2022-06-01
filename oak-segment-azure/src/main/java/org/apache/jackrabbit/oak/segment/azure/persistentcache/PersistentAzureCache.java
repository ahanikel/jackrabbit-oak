/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.jackrabbit.oak.segment.azure.persistentcache;

import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.AbstractPersistentCache;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;

public class PersistentAzureCache extends AbstractPersistentCache {

    public static int BLOB_SIZE_SAFETY_NET = 10_000_000;

    private static final Logger log = LoggerFactory.getLogger(PersistentAzureCache.class);

    private CloudBlobDirectory azure;
    private long lastWriteSegmentWarning;

    public PersistentAzureCache(CloudBlobDirectory azure) {
        this.azure = azure;
        this.lastWriteSegmentWarning = 0;
    }

    @Override
    @Nullable
    protected Buffer readSegmentInternal(long msb, long lsb) {
        try {
            final CloudBlockBlob blob = azure.getBlockBlobReference(segmentIdToString(msb, lsb));
            blob.downloadAttributes();
            final int size = Math.toIntExact(Math.min(blob.getProperties().getLength(), BLOB_SIZE_SAFETY_NET));
            final Buffer ret = Buffer.allocate(size);
            blob.downloadToByteArray(ret.array(), 0);
            ret.limit(size);
            return ret;
        } catch (Exception e) {
            log.trace(e.toString());
            return null;
        }
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        try {
            final CloudBlockBlob blob = azure.getBlockBlobReference(segmentIdToString(msb, lsb));
            return blob.exists();
        } catch (Exception e) {
            log.trace(e.toString());
        }
        return false;
    }

    @Override
    public void writeSegment(long msb, long lsb, Buffer buffer) {
        try {
            buffer.rewind();
            int size = buffer.remaining();
            final CloudBlockBlob blob = azure.getBlockBlobReference(segmentIdToString(msb, lsb));
            final byte[] bytes = new byte[size]; // TODO: is there a better way?
            buffer.get(bytes);
            blob.upload(new ByteArrayInputStream(bytes), bytes.length);
            blob.uploadProperties();
        } catch (Exception e) {
            if (System.nanoTime() - lastWriteSegmentWarning > 60_000_000_000L) {
                lastWriteSegmentWarning = System.nanoTime();
                log.warn(e.toString());
            }
        }
    }

    @Override
    public void cleanUp() {
        try {
            for (ListBlobItem blob : azure.listBlobs("")) {
                ((CloudBlockBlob) blob).delete();
            }
        } catch (Exception e) {
            log.error(e.toString());
        }
    }

    static String segmentIdToString(long msb, long lsb) {
        final String ret = String.format("%016x%016x", msb, lsb);
        return ret;
    }
}
