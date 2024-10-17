package org.apache.jackrabbit.oak.store.zeromq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class ZeroMQBlobStore implements RemoteBlobStore {
    private static final Logger log = LoggerFactory.getLogger(ZeroMQBlobStore.class.getName());
    private final SimpleRequestResponse queueReader;
    private final SimpleRequestResponse queueWriter;

    public ZeroMQBlobStore(SimpleRequestResponse queueReader, SimpleRequestResponse queueWriter) {
        this.queueReader = queueReader;
        this.queueWriter = queueWriter;
    }

    @Override
    public boolean hasBlob(String uuid) {
        final String ret = queueReader.requestString("hasblob", uuid);
        if (!"E".equals(ret)) {
            return false;
        }
        return "true".equals(queueReader.receiveMore());
    }

    @Override
    public InputStream readBlob(String uuid) {
        return new ZeroMQBlobInputStream(queueReader, uuid);
    }

    @Override
    public void writeBlob(String ref, File file) throws FileNotFoundException {
        InputStream is = new FileInputStream(file);
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
}
