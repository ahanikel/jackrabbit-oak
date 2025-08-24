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

public class Constants {
  public static final String PARAM_JOURNAL_ID = "journalId";
  public static final String PARAM_BACKEND_READER_URL = "backendReaderURL";
  public static final String PARAM_BACKEND_WRITER_URL = "backendWriterURL";
  public static final String PARAM_INIT_JOURNAL = "initJournal";
  public static final String PARAM_BLOB_CACHE_DIR = "blobCacheDir";

  public static final String DEFAULT_JOURNAL_ID = "golden";
  public static final String DEFAULT_BACKEND_READER_URL = "tcp://localhost:8000";
  public static final String DEFAULT_BACKEND_WRITER_URL = "tcp://localhost:8001";
  public static final String DEFAULT_BLOB_CACHE_DIR = "/tmp/blobCacheDir";
}
