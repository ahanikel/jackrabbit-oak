package org.apache.jackrabbit.oak.store.zeromq;

import com.google.common.primitives.Longs;
import org.apache.jackrabbit.oak.api.Blob;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

public class ZeroMQBlob implements Blob {

    private final Supplier<File> fileSupplier;
    private String reference;

    private static final Logger log = LoggerFactory.getLogger(ZeroMQBlob.class);

    private ZeroMQBlob(String reference, Supplier<File> fileSupplier) {
        this.fileSupplier = fileSupplier;
        this.reference = reference;
    }

    static class InputStreamFileSupplier implements Supplier<File> {
        private volatile File file;
        private volatile String reference;
        private final InputStream is;
        private final File blobCacheDir;
        private static final ExecutorService readerThreads = Executors.newFixedThreadPool(10);
        private final CountDownLatch countDownLatch;

        InputStreamFileSupplier(@NotNull File blobCacheDir, @NotNull InputStream is) {
            checkNotNull(blobCacheDir);
            checkNotNull(is);
            this.blobCacheDir = blobCacheDir;
            this.is = is;
            countDownLatch = new CountDownLatch(1);
            readerThreads.execute(this::getInternal);
        }

        @Override
        public File get() {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
            }
            return file;
        }

        private void getInternal() {
            byte[] readBuffer = new byte[1024 * 1024]; // not final because of fear it's not being GC'd
            try {
                final MessageDigest md = MessageDigest.getInstance("MD5");
                final File out = File.createTempFile("zmqBlob", ".dat");
                final FileOutputStream fos = new FileOutputStream(out);
                final BufferedOutputStream bos = new BufferedOutputStream(fos);
                // The InputStream spec says that read reads at least one byte (if not eof),
                // reads 0 bytes only if buffer.length == 0,
                // and blocks if it's not available, but we're sending a 0-byte chunk to
                // terminate the "sendMore" sequence.
                for (int nRead = is.read(readBuffer); nRead >= 0; nRead = is.read(readBuffer)) {
                    bos.write(readBuffer, 0, nRead);
                    md.update(readBuffer, 0, nRead);
                }
                bos.flush();
                bos.close();
                fos.flush();
                fos.close();
                is.close();
                reference = uuidFromBytes(md.digest());
                file = getFileForUuid(blobCacheDir, reference);
                synchronized (ZeroMQBlob.class) {
                    if (file.exists()) {
                        out.delete();
                    } else {
                        out.renameTo(file);
                    }
                }
            } catch (Exception e) {
                log.error(e.toString());
                throw new IllegalStateException(e);
            }
            countDownLatch.countDown();
        }

        public String getReference() {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
            }
            return reference;
        }
    }

    /*
        Check if a blob already exists in the filesystem.
        @return an instance of ZeroMQBlob if it exists, otherwise null.
    */
    @Nullable
    public static ZeroMQBlob newInstance(File blobCacheDir, String reference) {
        File destFile = getFileForUuid(blobCacheDir, reference);
        if (destFile.exists()) {
            return new ZeroMQBlob(reference, () -> destFile);
        }
        return null;
    }

    public static ZeroMQBlob newInstance(File blobCacheDir, String reference, File f) {
        try {
            File destFile = getFileForUuid(blobCacheDir, reference);
            synchronized (ZeroMQBlob.class) {
                if (destFile.exists()) {
                    f.delete();
                } else {
                    f.renameTo(destFile);
                }
            }
            return new ZeroMQBlob(reference, () -> destFile);
        } catch (Exception e) {
            log.error(e.toString());
            throw new IllegalStateException(e);
        }
    }

    static ZeroMQBlob newInstance(@NotNull File blobCacheDir, String reference, @NotNull InputStream is) {
        checkNotNull(blobCacheDir);
        checkNotNull(is);
        InputStreamFileSupplier fileSupplier = new InputStreamFileSupplier(blobCacheDir, is);
        if (!reference.equals(fileSupplier.getReference())) {
            log.warn("Requested reference {} does not match calculated reference {}.", reference, fileSupplier.getReference());
        }
        return new ZeroMQBlob(fileSupplier.getReference(), fileSupplier);
    }

    static ZeroMQBlob newInstance(@NotNull File blobCacheDir, @NotNull InputStream is) {
        checkNotNull(is);
        InputStreamFileSupplier fileSupplier = new InputStreamFileSupplier(blobCacheDir, is);
        return new ZeroMQBlob(fileSupplier.getReference(), fileSupplier);
    }

    private static File getFileForUuid(File blobCacheDir, String uuid) {
        uuid = uuid.toLowerCase();
        return new File(blobCacheDir, uuid);
    }

    @Override
    public @NotNull InputStream getNewStream() {
        try {
            return new FileInputStream(fileSupplier.get());
        } catch (FileNotFoundException e) {
            log.error(e.toString());
            throw new IllegalStateException("Unable to create blob stream");
        }
    }

    @Override
    public long length() {
        return fileSupplier.get().length();
    }

    @Override
    public @Nullable String getReference() {
        return this.reference;
    }

    // This is for the very special case where we create a blob that
    // could not be loaded. TODO: not sure if that's a good idea
    public void setReference(String reference) {
        this.reference = reference;
    }

    @Override
    public @Nullable String getContentIdentity() {
        return this.reference;
    }

    private static String uuidFromBytes(byte[] bytes) {
        final long msb = Longs.fromByteArray(Arrays.copyOfRange(bytes, 0, 8));
        final long lsb = Longs.fromByteArray(Arrays.copyOfRange(bytes, 8, 16));
        return new UUID(msb, lsb).toString();
    }
}
