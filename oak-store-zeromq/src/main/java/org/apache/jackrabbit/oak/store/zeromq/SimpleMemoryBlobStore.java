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
