package org.apache.jackrabbit.oak.store.zeromq;

import org.apache.jackrabbit.oak.api.Blob;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;

public class SimpleBlob implements Blob {

    private final BlobStore store;
    private final String ref;

    public SimpleBlob(BlobStore store, String ref) {
        this.store = store;
        this.ref = ref;
    }

    @Override
    public @NotNull InputStream getNewStream() {
        try {
            return store.getInputStream(ref);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public long length() {
        try {
            return store.getLength(ref);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public @Nullable String getReference() {
        return ref;
    }

    @Override
    public @Nullable String getContentIdentity() {
        return ref;
    }

    @Override
    public boolean isInlined() {
        return false;
    }
}
