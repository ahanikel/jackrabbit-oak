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

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PersistentAzureCacheManualTest {

    @Ignore("for manual testing")
    @Test
    public void testCacheManually() throws URISyntaxException, InvalidKeyException, StorageException {
        String connectionURL = System.getenv("AZURE_STORAGE_CONNECTION_STRING");
        CloudStorageAccount storageAccount = CloudStorageAccount.parse(connectionURL);
        CloudBlobContainer container = storageAccount.createCloudBlobClient().getContainerReference("aem-cache");
        container.createIfNotExists();
        CloudBlobDirectory directory = container.getDirectoryReference("aem-cache");
        PersistentAzureCache persistentAzureCache = new PersistentAzureCache(directory);
        persistentAzureCache.cleanUp();
        final String hello = "Hello, world!";
        assertFalse(persistentAzureCache.containsSegment(1, 1));
        final Buffer buffer = Buffer.wrap(hello.getBytes());
        persistentAzureCache.writeSegment(1, 1, buffer);
        assertTrue(persistentAzureCache.containsSegment(1, 1));
        Buffer buffer1 = persistentAzureCache.readSegmentInternal(1, 1);
        final byte[] bytes = buffer1.array();
        assertEquals(hello, new String(bytes));

        // now test with segment size, starting with a 1 bit.
        final byte[] bigger = new byte[256*1024];
        for (int i = 0; i < bigger.length; i += 2) {
            bigger[i] = (byte) 0xf0;
        }
        persistentAzureCache.writeSegment(Integer.MAX_VALUE, Integer.MIN_VALUE, Buffer.wrap(bigger));
        Buffer buffer2 = persistentAzureCache.readSegmentInternal(Integer.MAX_VALUE, Integer.MIN_VALUE);
        final byte[] bytes2 = buffer2.array();
        assertArrayEquals(bigger, bytes2);
    }
}
