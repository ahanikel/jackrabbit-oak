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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public interface BlobStore {
    byte[] getBytes(String uuid) throws IOException;
    String getString(String uuid) throws IOException;
    FileInputStream getInputStream(String uuid) throws FileNotFoundException;
    void putString(String uuid, String string) throws IOException;
    void putBytes(String uuid, byte[] bytes) throws IOException;
    void putInputStream(String uuid, InputStream is) throws IOException;

    void putTempFile(String uuid, File tempFile);

    boolean hasBlob(String uuid);
    long size();
}
