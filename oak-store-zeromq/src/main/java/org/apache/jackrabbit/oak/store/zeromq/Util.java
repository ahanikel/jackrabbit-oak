/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.store.zeromq;

import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Util {

    public static final Logger LOG = org.slf4j.LoggerFactory.getLogger(Util.class);

    public static byte[] LONG_ZERO = longToBytes(0L);

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
                while (nRead >= 0) {
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

    public static byte[] longToBytes(long l) {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
        buf.putLong(l);
        return buf.array();
    }

    public static long longFromBytes(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
        buf.put(bytes);
        buf.rewind();
        return buf.getLong();
    }

    public static String fromFile(String path) {
        try {
            return Files.readString(Paths.get(path));
        } catch (IOException e) {
            LOG.error("Got IOException while reading from file " + path + " : " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
