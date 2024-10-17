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

public class SimpleRemoteBlobStore implements BlobStore {

    private final SimpleBlobStore localCache;
    private final RemoteBlobStore remoteBlobStore;

    public SimpleRemoteBlobStore(SimpleBlobStore localCache, RemoteBlobStore remoteBlobStore) {
        this.localCache = localCache;
        this.remoteBlobStore = remoteBlobStore;
    }

    private void ensureBlobInCache(String ref) throws IOException {
        if (!localCache.hasBlob(ref)) {
            try {
                localCache.putInputStream(remoteBlobStore.readBlob(ref));
            } catch (BlobAlreadyExistsException e) {
                // should not happen
            }
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
    public FileInputStream getInputStream(String ref) throws IOException {
        ensureBlobInCache(ref);
        return localCache.getInputStream(ref);
    }

    @Override
    public String putBytes(byte[] bytes) throws IOException, BlobAlreadyExistsException {
        final String ref = localCache.putBytes(bytes);
        if (!remoteBlobStore.hasBlob(ref)) {
            remoteBlobStore.writeBlob(ref, localCache.getFile(ref));
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
        if (!remoteBlobStore.hasBlob(ref)) {
            remoteBlobStore.writeBlob(ref, localCache.getFile(ref));
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
        remoteBlobStore.writeBlob(ref, localCache.getFile(ref));
        return ref;
    }

    @Override
    public File getSpecificFile(String name) {
        throw new UnsupportedOperationException("Does not work for remote blob stores, a File is local by definition.");
    }

    @Override
    public boolean hasBlob(String ref) throws IOException {
        if (!localCache.hasBlob(ref)) {
            try {
                InputStream is = remoteBlobStore.readBlob(ref);
                if (is == null) {
                    return false;
                }
                localCache.putInputStream(is);
            } catch (BlobAlreadyExistsException e) {
                // should not happen
            }
        }
        return true;
    }

    @Override
    public long getLength(String ref) throws IOException {
        ensureBlobInCache(ref);
        return localCache.getLength(ref);
    }
}
