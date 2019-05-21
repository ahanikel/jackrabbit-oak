package org.apache.jackrabbit.oak.store.zeromq;

import org.apache.jackrabbit.oak.api.Blob;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;

public class SimpleBlob implements Blob {

    private final SimpleNodeStore store;
    private final String ref;

    @NotNull
    public static SimpleBlob get(SimpleNodeStore store, String ref) {
        return new SimpleBlob(store, ref);
    }

    private SimpleBlob(SimpleNodeStore store, String ref) {
        this.store = store;
        this.ref = ref;
    }

    @Override
    public @NotNull InputStream getNewStream() {
        try {
            return store.getRemoteBlobStore().getInputStream(ref);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public long length() {
        try {
            return store.getRemoteBlobStore().getLength(ref);
        } catch (IOException e) {
            throw new IllegalStateException(e);
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
