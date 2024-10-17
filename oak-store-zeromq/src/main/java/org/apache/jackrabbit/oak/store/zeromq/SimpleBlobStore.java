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

import java.io.ByteArrayInputStream;
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
    public byte[] getBytes(String ref) throws IOException {
        try (final InputStream is = getInputStream(ref)) {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            IOUtils.copy(is, bos);
            return bos.toByteArray();
        }
    }

    @Override
    public String getString(String ref) throws IOException {
        return new String(getBytes(ref));
    }

    @Override
    public FileInputStream getInputStream(String ref) throws FileNotFoundException {
        checkRef(ref);
        if (ref == null || ref.length() < 6) {
            throw new FileNotFoundException("ref: " + ref);
        }
        return new FileInputStream(getFileForRef(ref));
    }

    public File getFile(String ref) throws FileNotFoundException {
        checkRef(ref);
        if (ref == null || ref.length() < 6) {
            throw new FileNotFoundException("ref: " + ref);
        }
        return getFileForRef(ref);
    }
    
    @Override
    public String putBytes(byte[] bytes) throws IOException, BlobAlreadyExistsException {
        return putInputStream(new ByteArrayInputStream(bytes));
    }

    @Override
    public String putString(String string) throws IOException, BlobAlreadyExistsException {
        return putBytes(string.getBytes());
    }

    @Override
    public String putInputStream(InputStream is) throws BlobAlreadyExistsException, IOException {
        final File tempFile = getTempFile();
        try (OutputStream os = new FileOutputStream(tempFile)) {
            IOUtils.copy(is, os);
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                // ignore
            }
        }
        return putTempFile(tempFile);
    }

    @Override
    public File getTempFile() throws IOException {
        return File.createTempFile("b64temp", ".dat", blobDir);
    }

    @Override
    public String putTempFile(File tempFile) throws BlobAlreadyExistsException {
        final String ref = Util.getRefFromFile(tempFile);
        if (hasBlob(ref)) {
            tempFile.delete();
            throw new BlobAlreadyExistsException(ref);
        } else {
            tempFile.renameTo(getFileForRef(ref));
        }
        return ref;
    }

    @Override
    public File getSpecificFile(String name) {
        checkRef(name);
        return new File(blobDir, name);
    }

    @Override
    public boolean hasBlob(String ref) {
        checkRef(ref);
        if (ref == null || ref.length() < 6) {
            return false;
        }
        return getFileForRef(ref).exists();
    }

    @Override
    public long getLength(String ref) throws IOException {
        checkRef(ref);
        return getFile(ref).length();
    }

    private File getFileForRef(String ref) {
        final StringBuilder dirName = new StringBuilder();
        dirName
            .append(ref.substring(0, 2))
            .append('/')
            .append(ref.substring(2, 4))
            .append('/')
            .append(ref.substring(4, 6));
        final File dir = new File(blobDir, dirName.toString());
        dir.mkdirs();
        return new File(dir, ref);
    }

    private void checkRef(String ref) {
        if (ref.contains("/")) {
            throw new IllegalArgumentException();
        }
    }
}
