package org.apache.jackrabbit.oak.store.zeromq;

import java.io.IOException;
import java.io.InputStream;
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

public class ZeroMQBlobStoreBlob extends AbstractBlob {

    private final BlobStore blobStore;

    private final String reference;

    public ZeroMQBlobStoreBlob(BlobStore blobStore, String reference) {
        this.blobStore = blobStore;
        this.reference = reference;
    }

    @Override
    public InputStream getNewStream() {
        try {
            return blobStore.getInputStream(reference);
        }
        catch (IOException ex) {
            return null;
        }
    }

    @Override
    public long length() {
        try {
            return blobStore.getBlobLength(reference);
        }
        catch (IOException ex) {
            return 0;
        }
    }

    @Override
    public String getReference() {
        return reference;
    }

}
