package org.apache.jackrabbit.oak.store.zeromq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.InputStream;

public class S3BlobStore implements RemoteBlobStore {
    private static final Logger log = LoggerFactory.getLogger(S3BlobStore.class.getName());
    private final String containerName;
    private final String folderName;

    private S3Client s3Client;

    public S3BlobStore(String endpoint, String signingRegion, String accessKey, String secretKey, String containerName, String folderName) {
        this.containerName = containerName;
        this.folderName = folderName + "/";
        AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        s3Client = S3Client.builder()
                .region(Region.of(signingRegion))
                .credentialsProvider(credentialsProvider)
                .build();
    }

    @Override
    public boolean hasBlob(String uuid) {
        return readBlob(uuid) != null;
    }

    @Override
    public InputStream readBlob(String uuid) {
        return s3Client.getObject(GetObjectRequest.builder().bucket(containerName).key(folderName + uuid).build());
    }

    @Override
    public void writeBlob(String uuid, File blob) {
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(containerName).key(folderName + uuid).build(), RequestBody.fromFile(blob));
    }
}
