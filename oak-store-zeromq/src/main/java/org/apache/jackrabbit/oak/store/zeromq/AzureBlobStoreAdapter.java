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

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlockBlobItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class AzureBlobStoreAdapter implements BlobStoreAdapter {
  private static final Logger log = LoggerFactory.getLogger(AzureBlobStoreAdapter.class.getName());
  private final BlobContainerClient containerClient;
  private final BlobContainerAsyncClient containerAsyncClient;
  private final List<Mono<BlockBlobItem>> uploadMonos = new ArrayList<Mono<BlockBlobItem>>(100);

  public AzureBlobStoreAdapter(String connectionString, String containerName) {
    BlobServiceClient serviceClient = new BlobServiceClientBuilder()
            .connectionString(connectionString)
            .buildClient();
    BlobServiceAsyncClient serviceAsyncClient = new BlobServiceClientBuilder()
            .connectionString(connectionString)
            .buildAsyncClient();
    this.containerClient = serviceClient.getBlobContainerClient(containerName);
    this.containerAsyncClient = serviceAsyncClient.getBlobContainerAsyncClient(containerName);
    containerClient.createIfNotExists();
  }

  private boolean hasBlob(String blobName) {
    return containerClient.getBlobClient(blobName).exists();
  }

  private InputStream readBlob(String blobName) {
    try {
      return new BufferedInputStream(containerClient.getBlobClient(blobName).openInputStream());
    } catch (Exception e) {
      if (e.getMessage() != null && e.getMessage().contains("404")) {
        return null;
      } else {
        throw e;
      }
    }
  }

  private void writeBlob(String blobName, InputStream inputStream) {
    try {
      if (blobName.contains("journal")) {
        while (!uploadMonos.isEmpty()) {
          uploadMonos.remove(0).block();
        }
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        blobClient.upload(BinaryData.fromStream(inputStream), true);
      } else {
        BlobAsyncClient blobClient = containerAsyncClient.getBlobAsyncClient(blobName);
        Mono<BlockBlobItem> upload = blobClient.upload(BinaryData.fromStream(inputStream), true);
        uploadMonos.add(upload);
      }
    } catch (Exception e) {
      log.error("Failed to upload blob: {}", e.getMessage());
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