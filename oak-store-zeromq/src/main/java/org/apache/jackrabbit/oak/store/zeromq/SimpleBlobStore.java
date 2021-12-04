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

import org.apache.jackrabbit.oak.commons.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class SimpleBlobStore implements BlobStore {

    private static String OFFSET_FILENAME = "offset";
    private static String OFFSET_FILENAME_NEW = OFFSET_FILENAME + ".new";

    private final File blobDir;
    private final File offsetFile;
    private final File offsetFileNew;
    private long offset;

    public SimpleBlobStore(File blobDir) throws IOException {
        this.blobDir = blobDir;
        offsetFile = new File(blobDir, OFFSET_FILENAME);
        offsetFileNew = new File(blobDir, OFFSET_FILENAME_NEW);
        init();
    }

    public void init() throws IOException {
        if (blobDir.exists()) {
            if (blobDir.isDirectory()) {
                if (blobDir.list().length == 0) {
                    initFromScratch();
                } else {
                    initFromExisting();
                }
            } else {
                throw new IllegalStateException("blobDir " + blobDir.getAbsolutePath() + " is not a directory.");
            }
        } else {
            blobDir.mkdirs();
            initFromScratch();
        }
    }

    private void initFromExisting() throws IOException {
        if (!offsetFile.exists()) {
            throw new IllegalStateException("blobDir " + blobDir.getAbsolutePath() + " is not empty but offset file does not exist");
        }
        if (offsetFileNew.exists()) {
            throw new IllegalStateException(OFFSET_FILENAME_NEW + " exists, recovery needed.");
        }
        offset = IOUtils.readLong(new FileInputStream(offsetFile));
    }

    private void initFromScratch() throws IOException {
        offset = 0L;
        writeOffset();
    }

    private void writeOffset() throws IOException {
        try (FileOutputStream fos = new FileOutputStream(offsetFile)) {
            IOUtils.writeLong(fos, offset);
        }
    }

    @Override
    public byte[] getBytes(String uuid) throws IOException {
        try (final InputStream is = getInputStream(uuid)) {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            IOUtils.copy(is, bos);
            return bos.toByteArray();
        }
    }

    @Override
    public String getString(String uuid) throws IOException {
        return new String(getBytes(uuid));
    }

    @Override
    public InputStream getInputStream(String uuid) throws FileNotFoundException {
        if (uuid == null || uuid.length() < 6) {
            throw new FileNotFoundException("uuid: " + uuid);
        }
        return new FileInputStream(getFileForUuid(uuid));
    }

    @Override
    public void putBytes(String uuid, byte[] bytes) throws IOException {
        try (OutputStream os = getOutputStream(uuid)) {
            if (os != null) {
                os.write(bytes);
            }
        }
    }

    @Override
    public void putString(String uuid, String string) throws IOException {
        try (OutputStream os = getOutputStream(uuid)) {
            if (os != null) {
                os.write(string.getBytes());
            }
        }
    }

    @Override
    public void putInputStream(String uuid, InputStream is) throws IOException {
        try (OutputStream os = getOutputStream(uuid)) {
            if (os != null) {
                IOUtils.copy(is, os);
            }
        }
    }

    @Override
    public boolean hasBlob(String uuid) {
        if (uuid == null || uuid.length() < 6) {
            return false;
        }
        return getFileForUuid(uuid).exists();
    }

    @Override
    public long size() {
        return 0;
    }

    private File getFileForUuid(String uuid) {
        return new File(blobDir, uuid);
    }

    private OutputStream getOutputStream(String uuid) throws FileNotFoundException {
        final File outputFile = getFileForUuid(uuid);
        // TODO: write to temp file first
        if (outputFile.exists()) {
            return null;
        }
        return new FileOutputStream(getFileForUuid(uuid));
    }
}
