package org.apache.jackrabbit.oak.store.zeromq;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ZeroMQBlob implements Blob {

    private final File file;
    private final String digest;
    private static final File blobCacheDir = new File("/tmp/blobs");

    private static final Logger log = LoggerFactory.getLogger(ZeroMQBlob.class);
    private static MessageDigest md;

    static {
        try {
            md = MessageDigest.getInstance("SHA-512");
            blobCacheDir.mkdir();
        } catch (NoSuchAlgorithmException e) {
            md = null;
        }
    }

    private ZeroMQBlob(File file, String digest) {
        this.file = file;
        this.digest = digest;
    }

    // Looks like we can't do that because there seem to be several ZeroMQBlob
    // instances per reference
    /*
    @Override
    protected void finalize() {
        synchronized (this.getClass()) {
            file.delete();
        }
    }
    */

    static void appendInputStream(File f, InputStream is) {
        try {
            final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(f, true));
            for (int nRead = is.read(); nRead >= 0; nRead = is.read()) {
                bos.write(nRead);
            }
            bos.flush();
            bos.close();
            is.close();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static void copyInto(byte[] src, byte[] dst) {
        copyInto(src, dst, 0);
    }

    static void copyInto(byte[] src, byte[] dst, int ofs) {
        if (src.length + ofs > dst.length) {
            throw new IllegalArgumentException("src + ofs is bigger than dst");
        }
        for (int i = 0; i < src.length; ++i) {
            dst[i + ofs] = src[i];
        }
    }

    static ZeroMQBlob newInstance(InputStream is) {
        final byte[] readBuffer = new byte[1024*1024];
        try {
            final MessageDigest md = MessageDigest.getInstance("SHA-512");
            final File out = File.createTempFile("zmqBlob", ".dat");
            final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(out));
            // The InflaterInputStream seems to take some time until it's ready
            if (is.available() == 0) {
                Thread.sleep(500);
            }
            // The InputStream spec says that read reads at least one byte (if not eof),
            // reads 0 bytes only if buffer.length == 0,
            // and blocks if it's not available, but I'm not sure if the InflaterInputStream
            // does that.
            for (int nRead = is.read(readBuffer); nRead >= 0; nRead = is.read(readBuffer)) {
                bos.write(readBuffer, 0, nRead);
                md.update(readBuffer, 0, nRead);
            }
            bos.flush();
            bos.close();
            is.close();
            final String reference = bytesToString(new ByteArrayInputStream(md.digest()));
            File destFile = new File("/tmp/blobs/", reference);
            synchronized (ZeroMQBlob.class) {
                if (destFile.exists()) {
                    out.delete();
                } else {
                    out.renameTo(destFile);
                }
            }
            return new ZeroMQBlob(destFile, reference);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static ZeroMQBlob newInstance(String reference, File f) {
        try {
            File destFile = new File("/tmp/blobs/", reference);
            synchronized (ZeroMQBlob.class) {
                if (destFile.exists()) {
                    f.delete();
                } else {
                    f.renameTo(destFile);
                }
            }
            return new ZeroMQBlob(destFile, reference);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static ZeroMQBlob newInstance(String reference) {
        final File file = new File(blobCacheDir, reference);
        synchronized (ZeroMQBlob.class) {
            if (file.exists()) {
                return new ZeroMQBlob(file, reference);
            }
        }
        return null;
    }

    /*
    static ZeroMQBlob newInstance(ZeroMQNodeStore ns, String reference) {
        try {
            final NodeState blobRoot = ns.getBlobRoot();
            final NodeState blobNode = blobRoot.getChildNode(reference);
            final PropertyState blobProp = blobNode.getProperty("blob");
            final String sBlob = blobProp.getValue(Type.STRING);
            return new ZeroMQBlob(bytesFromString(sBlob), reference);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
    */

    @Override
    public @NotNull InputStream getNewStream() {
        try {
            return new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("Unable to create blob stream");
        }
    }

    @Override
    public long length() {
        return file.length();
    }

    @Override
    public @Nullable String getReference() {
        return this.digest;
    }

    @Override
    public @Nullable String getContentIdentity() {
        return this.digest;
    }

    public String serialise() {
        try {
            return bytesToString(new FileInputStream(this.file));
        } catch (FileNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    public InputStream getStringStream() {
        return bytesToStringStream(getNewStream());
    }

    private static void appendInputStream(StringBuilder sb, InputStream is) {
        final char[] hex = "0123456789ABCDEF".toCharArray();
        int b;
        try {
            while ((b = is.read()) >= 0) {
                sb.append(hex[b >> 4]);
                sb.append(hex[b & 0x0f]);
            }
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static String bytesToString(InputStream is) {
        final StringBuilder sb = new StringBuilder();
        appendInputStream(sb, is);
        return sb.toString();
    }

    private static InputStream bytesToStringStream(InputStream is) {
        return new InputStream() {
            final char[] hex = "0123456789ABCDEF".toCharArray();
            volatile boolean isHiByte = true;
            volatile int b;
            @Override
            public int read() throws IOException {
                if (isHiByte) {
                    if ((b = is.read()) < 0) {
                        return -1;
                    }
                    isHiByte = false;
                    return hex[b >> 4];
                } else {
                    isHiByte = true;
                    return hex[b & 0x0f];
                }
            }
            @Override
            public int available() throws IOException {
                return is.available() * 2;
            }
        };
    }

    static File bytesFromInputStream(InputStream is) {
        final byte[] readBuffer = new byte[1024*1024*100]; // 100 MB
        try {
            final File out = File.createTempFile("zmqBlob", "dat");
            final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(out));
            // The InflaterInputStream seems to take some time until it's ready
            if (is.available() == 0) {
                Thread.sleep(500);
            }
            for (int nRead = is.read(readBuffer); nRead > 0; nRead = is.read(readBuffer)) {
                bos.write(readBuffer, 0, nRead);
            }
            bos.flush();
            bos.close();
            is.close();
            return out;
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    static File bytesFromString(String s) {
        final InputStream is = new InputStream() {
            char[] chars = s.toCharArray();
            volatile int cur = 0;

            private int hexCharToInt(char c) {
                return Character.isDigit(c) ? c - '0' : c - 'A' + 10;
            }

            private int next() {
                return hexCharToInt(chars[cur++]);
            }

            @Override
            public int read() throws IOException {
                if (cur >= chars.length - 1) {
                    return -1;
                }
                final int c = next();
                final int d = next();
                final int ret = c << 4 | d;
                return ret;
            }

            @Override
            public int available() {
                return (chars.length - cur) / 2;
            }
        };
        return bytesFromInputStream(is);
    }

    static InputStream bytesFromStringStream(InputStream is) {
        return new InputStream() {
            volatile int cur = 0;

            private int hexCharToInt(char c) {
                return Character.isDigit(c) ? c - '0' : c - 'A' + 10;
            }

            private int next() throws IOException {
                int n = is.read();
                if (n < 0) {
                    throw new EOFException();
                }
                return hexCharToInt((char) n);
            }

            @Override
            public int read() throws IOException {
                try {
                    final int c = next();
                    final int d = next();
                    final int ret = c << 4 | d;
                    return ret;
                } catch (EOFException e) {
                    return -1;
                }
            }

            @Override
            public int available() throws IOException {
                return is.available();
            }
        };
    }

    public static void streamCopy(InputStream is, OutputStream os) {
        try {
            for (int b = is.read(); b >= 0; b = is.read()) {
                os.write(b);
            }
            os.flush();
        } catch (Throwable e) {
            throw new IllegalStateException(e);
        }
    }
}
