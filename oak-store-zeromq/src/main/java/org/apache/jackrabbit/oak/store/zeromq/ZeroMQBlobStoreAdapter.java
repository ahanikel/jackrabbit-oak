package org.apache.jackrabbit.oak.store.zeromq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class ZeroMQBlobStoreAdapter implements BlobStoreAdapter {
    private static final Logger log = LoggerFactory.getLogger(ZeroMQBlobStoreAdapter.class.getName());
    private final SimpleRequestResponse queueReader;
    private final SimpleRequestResponse queueWriter;

    public ZeroMQBlobStoreAdapter(SimpleRequestResponse queueReader, SimpleRequestResponse queueWriter) {
        this.queueReader = queueReader;
        this.queueWriter = queueWriter;
    }

    private boolean hasBlob(String uuid) {
        final String ret = queueReader.requestString("hasblob", uuid);
        if (!"E".equals(ret)) {
            return false;
        }
        return "true".equals(queueReader.receiveMore());
    }

    private InputStream readBlob(String uuid) {
        return new ZeroMQBlobInputStream(queueReader, uuid);
    }

    private void writeBlob(String ref, InputStream is) {
        LoggingHook.writeBlob(ref, is, this::writeBlobChunk);
    }

    private void writeBlobChunk(String op, byte[] args) {
        while (true) {
            try {
                String msg = queueWriter.requestString(op, args);
                if (!msg.equals("E")) {
                    log.error("lastReq: {}", queueWriter.getLastReq());
                    log.error("{}: {}", msg, queueWriter.receiveMore());
                } else {
                    queueWriter.receiveMore(); // ignore, should be ""
                }
                break;
            } catch (Exception e1) {
                log.error(e1.getMessage());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
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
