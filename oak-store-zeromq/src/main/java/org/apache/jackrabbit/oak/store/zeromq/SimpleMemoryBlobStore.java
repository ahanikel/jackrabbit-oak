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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;

public class SimpleMemoryBlobStore implements BlobStore {

  private final Cache<String, byte[]> cache;

  public SimpleMemoryBlobStore() {
    this.cache = CacheBuilder.newBuilder().build();
  }

  public SimpleMemoryBlobStore(int maxSize) {
    this.cache = CacheBuilder.newBuilder().maximumSize(maxSize).build();
  }

  @Override
  public boolean hasBlob(String ref) {
    return cache.getIfPresent(ref) != null;
  }

  @Override
  public byte[] getBytes(String ref) throws IOException {
    byte[] bytes = cache.getIfPresent(ref);
    if (bytes == null) blobNotFound(ref);
    return bytes;
  }

  @Override
  public String getString(String ref) throws IOException {
    return new String(getBytes(ref));
  }

  @Override
  public InputStream getInputStream(String ref) throws IOException {
    return new ByteArrayInputStream(getBytes(ref));
  }

  @Override
  public String putBytes(byte[] bytes) throws IOException, BlobAlreadyExistsException {
    String ref = Util.getRefFromBytes(bytes);
    if (hasBlob(ref)) {
      throw new BlobAlreadyExistsException(ref);
    }
    cache.put(ref, bytes);
    return ref;
  }

  @Override
  public String putInputStream(InputStream is) throws IOException, BlobAlreadyExistsException {
    byte[] bytes = is.readAllBytes();
    return putBytes(bytes);
  }

  @Override
  public void putInputStreamAs(String ref, InputStream is) throws IOException {
    byte[] bytes = is.readAllBytes();
    cache.put(ref, bytes);
  }

  @Override
  public TemporaryBlob getTempBlob() throws IOException {
    return new MemoryTemporaryBlob();
  }

  @Override
  public String putTempBlob(TemporaryBlob tempBlob) throws BlobAlreadyExistsException, IOException {
    return putBytes(((ByteArrayOutputStream) tempBlob.getOutputStream()).toByteArray());
  }

  @Override
  public void putTempBlobAs(String ref, TemporaryBlob tempBlob) throws IOException {
    cache.put(ref, ((ByteArrayOutputStream) tempBlob.getOutputStream()).toByteArray());
  }

  @Override
  public long getLength(String ref) throws IOException {
    return getBytes(ref).length;
  }
}
