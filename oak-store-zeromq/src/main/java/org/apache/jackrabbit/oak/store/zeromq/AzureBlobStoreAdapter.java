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
 */

package org.apache.jackrabbit.oak.store.zeromq;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class AzureBlobStoreAdapter implements BlobStoreAdapter {
  private static final Logger log = LoggerFactory.getLogger(AzureBlobStoreAdapter.class.getName());
  private final BlobContainerClient containerClient;

  public AzureBlobStoreAdapter(String connectionString, String containerName) {
    BlobServiceClient serviceClient = new BlobServiceClientBuilder()
            .connectionString(connectionString)
            .buildClient();
    this.containerClient = serviceClient.getBlobContainerClient(containerName);
    containerClient.createIfNotExists();
  }

  private boolean hasBlob(String blobName) {
    long t0 = System.nanoTime();
    boolean result = containerClient.getBlobClient(blobName).exists();
    long ms = (System.nanoTime() - t0) / 1_000_000;
    log.debug("Azure hasBlob({}) = {} in {}ms", blobName, result, ms);
    if (ms > 500) {
      log.warn("Azure hasBlob slow: {} ms for {}", ms, blobName);
    }
    return result;
  }

  private InputStream readBlob(String blobName) {
    long t0 = System.nanoTime();
    try {
      InputStream is = new BufferedInputStream(containerClient.getBlobClient(blobName).openInputStream());
      log.debug("Azure readBlob({}) opened in {}ms", blobName, (System.nanoTime() - t0) / 1_000_000);
      return is;
    } catch (Exception e) {
      long ms = (System.nanoTime() - t0) / 1_000_000;
      if (e.getMessage() != null && e.getMessage().contains("404")) {
        log.debug("Azure readBlob({}) not found in {}ms", blobName, ms);
        return null;
      } else {
        log.error("Azure readBlob({}) failed after {}ms: {}", blobName, ms, e.getMessage());
        throw e;
      }
    }
  }

  private void writeBlob(String blobName, InputStream inputStream) {
    BlockBlobClient blobClient = containerClient.getBlobClient(blobName).getBlockBlobClient();
    if (!inputStream.markSupported()) {
      inputStream = new BufferedInputStream(inputStream);
    }
    long t0 = System.nanoTime();
    try {
      int size = inputStream.available();
      blobClient.upload(inputStream, size, true);
      long ms = (System.nanoTime() - t0) / 1_000_000;
      double kbPerSec = ms > 0 ? (size / 1024.0) / (ms / 1000.0) : Double.POSITIVE_INFINITY;
      log.info("Azure writeBlob({}) {} bytes in {}ms ({} KB/s)", blobName, size, ms, String.format("%.1f", kbPerSec));
    } catch (Exception e) {
      long ms = (System.nanoTime() - t0) / 1_000_000;
      log.error("Azure writeBlob({}) failed after {}ms: {}", blobName, ms, e.getMessage());
      throw new RuntimeException(e);
    }
  }

  @Override
  public Function<String, Boolean> getChecker() {
    return this::hasBlob;
  }

  @Override
  public Function<String, InputStream> getReader() {
    return this::readBlob;
  }

  @Override
  public BiConsumer<String, InputStream> getWriter() {
    return this::writeBlob;
  }
}