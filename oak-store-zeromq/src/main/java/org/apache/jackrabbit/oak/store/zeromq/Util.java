package org.apache.jackrabbit.oak.store.zeromq;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Util {

    public static String getRefFromBytes(byte[] b) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            return bytesToString(new ByteArrayInputStream(md5.digest(b)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String getRefFromString(String s) {
        return getRefFromBytes(s.getBytes());
    }

    public static String getRefFromFile(File file) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            try (InputStream is = new FileInputStream(file)) {
                byte[] buf = new byte[1024 * 1024];
                int nRead = is.read(buf);
                while (nRead > 0) {
                    md5.update(buf, 0, nRead);
                    nRead = is.read(buf);
                }
            }
            return bytesToString(new ByteArrayInputStream(md5.digest()));
        } catch (NoSuchAlgorithmException | IOException e) {
            throw new IllegalStateException(e);
        }
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
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    private static String bytesToString(InputStream is) {
        final StringBuilder sb = new StringBuilder();
        appendInputStream(sb, is);
        return sb.toString();
    }
}
