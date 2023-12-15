package org.apache.jackrabbit.oak.store.zeromq;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * TODO: We are using the azure-storage-blob client out of laziness here
 *       but we should eventually use the HTTP API instead
 */
public class AzureBlobStoreAdapter implements BlobStoreAdapter {
    private static final Logger log = LoggerFactory.getLogger(AzureBlobStoreAdapter.class.getName());
    private final BlobServiceClient blobServiceClient;
    private final BlobContainerClient blobContainerClient;

    public AzureBlobStoreAdapter(String azureConnectionString, String containerName) {
        blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(azureConnectionString)
                .buildClient();
        blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
        blobContainerClient.createIfNotExists();
    }

    private boolean hasBlob(String uuid) {
        return blobContainerClient.getBlobClient(uuid).exists();
    }

    private InputStream readBlob(String uuid) {
        return blobContainerClient.getBlobClient(uuid).openInputStream();
    }

    private void writeBlob(String uuid, InputStream is) {
        blobContainerClient.getBlobClient(uuid).upload(is);
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
