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

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class SimpleRemoteBlobStore implements BlobStore {

    private final Function<String, Boolean> checker;
    private final Function<String, InputStream> reader;
    private final BiConsumer<String, InputStream> writer;
    private final BlobStore localCache;

    public SimpleRemoteBlobStore(Function<String, Boolean> checker, Function<String, InputStream> reader,
                                 BiConsumer<String, InputStream> writer, BlobStore localCache) {
        this.checker = checker;
        this.reader = reader;
        this.writer = writer;
        this.localCache = localCache;
    }

    private void ensureBlobInCache(String ref) throws IOException {
        if (ref.contains("journal") || !localCache.hasBlob(ref)) {
          InputStream is = reader.apply(ref);
          if (is == null) {
              throw new FileNotFoundException("Blob not found: " + ref);
          }
          localCache.putInputStreamAs(ref, is);
        }
    }

    @Override
    public byte[] getBytes(String ref) throws IOException {
        ensureBlobInCache(ref);
        return localCache.getBytes(ref);
    }

    @Override
    public String getString(String ref) throws IOException {
        ensureBlobInCache(ref);
        return localCache.getString(ref);
    }

    @Override
    public InputStream getInputStream(String ref) throws IOException {
        ensureBlobInCache(ref);
        return localCache.getInputStream(ref);
    }

    @Override
    public String putBytes(byte[] bytes) throws IOException, BlobAlreadyExistsException {
        final String ref = localCache.putBytes(bytes);
        if (!checker.apply(ref)) {
            writer.accept(ref, new ByteArrayInputStream(bytes));
        }
        return ref;
    }

    @Override
    public String putInputStream(InputStream is) throws IOException, BlobAlreadyExistsException {
        final String ref = localCache.putInputStream(is);
        if (!checker.apply(ref)) {
            writer.accept(ref, localCache.getInputStream(ref));
        }
        return ref;
    }

    @Override
    public void putInputStreamAs(String ref, InputStream is) throws IOException {
        localCache.putInputStreamAs(ref, is);
        if (ref.contains("journal") || !checker.apply(ref)) {
            writer.accept(ref, localCache.getInputStream(ref));
        }
    }

    @Override
    public TemporaryBlob getTempBlob() throws IOException {
        return localCache.getTempBlob();
    }

    @Override
    public String putTempBlob(TemporaryBlob tempFile) throws BlobAlreadyExistsException, IOException {
        final String ref = localCache.putTempBlob(tempFile);
        writer.accept(ref, localCache.getInputStream(ref));
        return ref;
    }

    @Override
    public void putTempBlobAs(String ref, TemporaryBlob tempBlob) throws IOException {
        localCache.putTempBlobAs(ref, tempBlob);
        if (ref.contains("journal") || !checker.apply(ref)) {
            writer.accept(ref, localCache.getInputStream(ref));
        }
    }

    @Override
    public boolean hasBlob(String ref) {
        // return localCache.hasBlob(ref) || checker.apply(ref); // more efficient but dangerous
        return checker.apply(ref);

    }

    @Override
    public long getLength(String ref) throws IOException {
        ensureBlobInCache(ref);
        return localCache.getLength(ref);
    }
}
