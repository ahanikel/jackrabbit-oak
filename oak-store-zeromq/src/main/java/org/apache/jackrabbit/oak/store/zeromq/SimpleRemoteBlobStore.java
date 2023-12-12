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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class SimpleRemoteBlobStore implements BlobStore {

    private final Function<String, Boolean> checker;
    private final Function<String, InputStream> reader;
    private final BiConsumer<String, InputStream> writer;
    private final SimpleBlobStore localCache;

    public SimpleRemoteBlobStore(Function<String, Boolean> checker, Function<String, InputStream> reader,
                                 BiConsumer<String, InputStream> writer, SimpleBlobStore localCache) {
        this.checker = checker;
        this.reader = reader;
        this.writer = writer;
        this.localCache = localCache;
    }

    private void ensureBlobInCache(String ref) throws IOException {
        if (!localCache.hasBlob(ref)) {
            try {
                localCache.putInputStream(reader.apply(ref));
            } catch (BlobAlreadyExistsException e) {
                // should not happen
            }
        }
    }

    // the check has already been done at this point
    private void writeBytesRemote(String ref, byte[] bytes) {
        writer.accept(ref, new ByteArrayInputStream(bytes));
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
    public FileInputStream getInputStream(String ref) throws IOException {
        ensureBlobInCache(ref);
        return localCache.getInputStream(ref);
    }

    @Override
    public String putBytes(byte[] bytes) throws IOException, BlobAlreadyExistsException {
        final String ref = localCache.putBytes(bytes);
        if (!checker.apply(ref)) {
            writeBytesRemote(ref, bytes);
        }
        return ref;
    }

    @Override
    public String putString(String string) throws IOException, BlobAlreadyExistsException {
        return putBytes(string.getBytes());
    }

    @Override
    public String putInputStream(InputStream is) throws IOException, BlobAlreadyExistsException {
        final String ref = localCache.putInputStream(is);
        if (!checker.apply(ref)) {
            writeBytesRemote(ref, localCache.getBytes(ref));
        }
        return ref;
    }

    @Override
    public File getTempFile() throws IOException {
        return localCache.getTempFile();
    }

    @Override
    public String putTempFile(File tempFile) throws BlobAlreadyExistsException, IOException {
        final String ref = localCache.putTempFile(tempFile);
        writer.accept(ref, localCache.getInputStream(ref));
        return ref;
    }

    @Override
    public File getSpecificFile(String name) {
        throw new UnsupportedOperationException("Does not work for remote blob stores, a File is local by definition.");
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
