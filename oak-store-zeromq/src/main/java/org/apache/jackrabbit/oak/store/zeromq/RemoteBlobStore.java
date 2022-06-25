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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class RemoteBlobStore implements BlobStore {

    private final Function<String, InputStream> reader;
    private final BiConsumer<String, String> writer;
    private final SimpleBlobStore localStore;

    public RemoteBlobStore(Function<String, InputStream> reader, BiConsumer<String,String> writer, SimpleBlobStore localStore) {
        this.reader = reader;
        this.writer = writer;
        this.localStore = localStore;
    }

    private void ensureBlob(String ref) throws IOException {
        if (!localStore.hasBlob(ref)) {
            try {
                localStore.putInputStream(reader.apply(ref));
            } catch (BlobAlreadyExistsException e) {
                // should not happen
            }
        }
    }

    @Override
    public byte[] getBytes(String ref) throws IOException {
        ensureBlob(ref);
        return localStore.getBytes(ref);
    }

    @Override
    public String getString(String ref) throws IOException {
        ensureBlob(ref);
        return localStore.getString(ref);
    }

    @Override
    public FileInputStream getInputStream(String ref) throws IOException {
        ensureBlob(ref);
        return localStore.getInputStream(ref);
    }

    private void writeBlobRemote(String ref) throws IOException {
        LoggingHook.writeBlob(ref, localStore.getInputStream(ref), writer);
    }

    @Override
    public String putBytes(byte[] bytes) throws IOException, BlobAlreadyExistsException {
        final String ref = localStore.putBytes(bytes);
        if (reader.apply(ref) == null) {
            writeBlobRemote(ref);
        }
        return ref;
    }

    @Override
    public String putString(String string) throws IOException, BlobAlreadyExistsException {
        return putBytes(string.getBytes());
    }

    @Override
    public String putInputStream(InputStream is) throws IOException, BlobAlreadyExistsException {
        final String ref = localStore.putInputStream(is);
        if (reader.apply(ref) == null) {
            writeBlobRemote(ref);
        }
        return ref;
    }

    @Override
    public File getTempFile() throws IOException {
        return localStore.getTempFile();
    }

    @Override
    public String putTempFile(File tempFile) throws BlobAlreadyExistsException, IOException {
        final String ref = localStore.putTempFile(tempFile);
        writeBlobRemote(ref);
        return ref;
    }

    @Override
    public File getSpecificFile(String name) {
        throw new UnsupportedOperationException("Does not work for remote blob stores, a File is local by definition.");
    }

    @Override
    public boolean hasBlob(String ref) {
        try {
            ensureBlob(ref);
        } catch (IOException e) {
            return false;
        }
        return localStore.hasBlob(ref);
    }

    @Override
    public long getLength(String ref) throws IOException {
        ensureBlob(ref);
        return localStore.getLength(ref);
    }
}
