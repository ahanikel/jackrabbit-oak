package org.apache.jackrabbit.oak.store.zeromq;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.BiConsumer;
import java.util.function.Function;

public interface RemoteBlobStore {
    public boolean hasBlob(String ref) throws IOException;
    public InputStream readBlob(String ref) throws IOException;
    public void writeBlob(String ref, File file) throws IOException;
}
