package org.apache.jackrabbit.oak.store.zeromq;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ZeroMQBlob implements Blob {

    private final byte[] bytes;
    private final String digest;

    private ZeroMQBlob(byte[] bytes) {
        this.bytes = bytes;
        this.digest = getDigest();
    }

    static ZeroMQBlob newInstance(byte[] bytes) {
        return new ZeroMQBlob(bytes);
    }

    static ZeroMQBlob newInstance(InputStream is) {
        return newInstance(bytesFromInputStream(is));
    }

    static ZeroMQBlob newInstance(ZeroMQNodeStore ns, String reference) {
        final String sBlob = ns.getBlobRoot().getChildNode(reference).getProperty("blob").getValue(Type.STRING);
        return newInstance(bytesFromString(sBlob));
    }

    static ZeroMQBlob newInstance(String serialised) {
        return newInstance(bytesFromString(serialised));
    }

    @Override
    public @NotNull InputStream getNewStream() {
        return new ByteArrayInputStream(bytes);
    }

    @Override
    public long length() {
        return bytes.length;
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
        return bytesToString(this.bytes);
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

    private static String bytesToString(byte[] bytes) {
        final StringBuilder sb = new StringBuilder();
        final InputStream is = new ByteArrayInputStream(bytes);
        appendInputStream(sb, is);
        return sb.toString();
    }

    static byte[] bytesFromInputStream(InputStream is) {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            for (int b = is.read(); b >= 0; b = is.read()) {
                bos.write(b);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return bos.toByteArray();
    }

    static byte[] bytesFromString(String s) {
        final InputStream is = new InputStream() {
            char[] chars = s.toCharArray();
            int cur = 0;

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
        };
        return bytesFromInputStream(is);
    }

    private String getDigest() {
        final MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-512");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        return bytesToString(md.digest(bytes));
    }
}
