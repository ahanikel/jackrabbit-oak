package org.apache.jackrabbit.oak.store.zeromq;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class S3BlobStoreAdapter implements BlobStoreAdapter {
    private static final Logger log = LoggerFactory.getLogger(S3BlobStoreAdapter.class.getName());
    private final String containerName;
    private final String folderName;

    private AmazonS3 s3Client;

    public S3BlobStoreAdapter(String endpoint, String signingRegion, String accessKey, String secretKey, String containerName, String folderName) {
        this.containerName = containerName;
        this.folderName = folderName + "/";
        AWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);
        s3Client = AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, signingRegion))
                .withCredentials(new AWSStaticCredentialsProvider(creds))
                .build();
    }

    private boolean hasBlob(String uuid) {
        return s3Client.doesObjectExist(containerName, folderName + uuid);
    }

    private InputStream readBlob(String uuid) {
        return s3Client.getObject(containerName, folderName + uuid).getObjectContent();
    }

    private void writeBlob(String uuid, InputStream is) {
        s3Client.putObject(containerName, folderName + uuid, is, new ObjectMetadata());
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
