package org.apache.jackrabbit.oak.store.zeromq;

import java.io.File;
import java.io.InputStream;
import java.util.function.BiConsumer;
import java.util.function.Function;

public interface BlobStoreAdapter {
    public Function<String, Boolean> getChecker();
    public Function<String, InputStream> getReader();
    public BiConsumer<String, File> getWriter();
}
