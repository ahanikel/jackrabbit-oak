package org.apache.jackrabbit.oak.store.zeromq.cli;

import org.apache.jackrabbit.oak.store.zeromq.AzureBlobStoreAdapter;
import org.apache.jackrabbit.oak.store.zeromq.BlobStore;
import org.apache.jackrabbit.oak.store.zeromq.Constants;
import org.apache.jackrabbit.oak.store.zeromq.SimpleBlobStore;
import org.apache.jackrabbit.oak.store.zeromq.SimpleBlobWriterService;
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

  @Override
  public void run() {
    try {
      BlobStore blobStore = new SimpleBlobStore(blobDir);
      if (azureConnectionString != null && !azureConnectionString.isEmpty()) {
        // Initialize Azure Blob Storage if connection string is provided
        AzureBlobStoreAdapter adapter = new AzureBlobStoreAdapter(azureConnectionString, containerName);
        blobStore = new SimpleRemoteBlobStore(adapter.getChecker(),
                adapter.getReader(),
                adapter.getWriter(),
                (SimpleBlobStore) blobStore);
      }
      new SimpleBlobWriterService(blobStore, sendingUri, receivingUri).run();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
