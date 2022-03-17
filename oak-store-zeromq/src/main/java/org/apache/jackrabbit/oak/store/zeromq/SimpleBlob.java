package org.apache.jackrabbit.oak.store.zeromq;

import org.apache.jackrabbit.oak.api.Blob;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.FileNotFoundException;
import java.io.InputStream;

public class SimpleBlob implements Blob {

    private final SimpleBlobStore store;
    private final String ref;

    public SimpleBlob(SimpleBlobStore store, String ref) {
        this.store = store;
        this.ref = ref;
    }

    @Override
    public @NotNull InputStream getNewStream() {
        try {
            return store.getInputStream(ref);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public long length() {
        try {
            return store.getFile(ref).length();
        } catch (FileNotFoundException e) {
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
