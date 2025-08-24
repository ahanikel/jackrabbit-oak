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
package org.apache.jackrabbit.oak.store.zeromq.cli;

import org.apache.jackrabbit.oak.store.zeromq.AzureBlobStoreAdapter;
import org.apache.jackrabbit.oak.store.zeromq.BlobStore;
import org.apache.jackrabbit.oak.store.zeromq.Constants;
import org.apache.jackrabbit.oak.store.zeromq.SimpleBlobReaderService;
import org.apache.jackrabbit.oak.store.zeromq.SimpleBlobStore;
import org.apache.jackrabbit.oak.store.zeromq.SimpleBlobWriterService;
import org.apache.jackrabbit.oak.store.zeromq.SimpleMemoryBlobStore;
import org.apache.jackrabbit.oak.store.zeromq.SimpleRemoteBlobStore;
import picocli.CommandLine;

import java.io.File;

@CommandLine.Command(
        name = "blob-writer",
        mixinStandardHelpOptions = true,
description = "Run the blob writer service")
public class BlobWriterCommand implements Runnable {

  @CommandLine.Option(names = {"-s", "--sending-uri"}, description = "URI listening for messages (default: ${DEFAULT-VALUE})")
  String sendingUri = Constants.DEFAULT_BACKEND_WRITER_URL;

  @CommandLine.Option(names = {"-r", "--receiving-uri"}, description = "URI sending messages to (default: ${DEFAULT-VALUE})")
  String receivingUri = Constants.DEFAULT_BACKEND_READER_URL;

  @CommandLine.Option(names = {"-d", "--blob-dir"}, description = "Directory where the blobs are stored / cached")
  File blobDir;

  @CommandLine.Option(names = {"-a", "--azure-connection-string"}, description = "Azure connection string for blob storage")
  String azureConnectionString;

  @CommandLine.Option(names = {"-c", "--container-name"}, description = "Azure Blob Storage container name (default: ${DEFAULT-VALUE})")
  String containerName = "zmq-blobstore";

  @CommandLine.Option(names = {"-m", "--memory-blob-store"}, description = "Use an in-memory blob store instead of file-based storage")
  boolean useMemoryBlobStore = false;

  @Override
  public void run() {
    try {
      BlobStore blobStore;
      if (useMemoryBlobStore) {
        blobStore = new SimpleMemoryBlobStore();
        new Thread(null, new SimpleBlobReaderService(blobStore, sendingUri, receivingUri), "Blob Reader").start();
      } else {
        blobStore = new SimpleBlobStore(blobDir);
        if (azureConnectionString != null && !azureConnectionString.isEmpty()) {
          // Initialize Azure Blob Storage if connection string is provided
          AzureBlobStoreAdapter adapter = new AzureBlobStoreAdapter(azureConnectionString, containerName);
          blobStore = new SimpleRemoteBlobStore(adapter.getChecker(),
                  adapter.getReader(),
                  adapter.getWriter(),
                  (SimpleBlobStore) blobStore);
        }
      }
      new SimpleBlobWriterService(blobStore, sendingUri, receivingUri).run();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
