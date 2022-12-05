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
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

public class SimpleNodeStoreBuilder {

    private static final Logger log = LoggerFactory.getLogger(SimpleNodeStoreBuilder.class.getName());

    public static final String PARAM_JOURNAL_ID = "journalId";
    public static final String PARAM_BACKEND_READER_URL = "backendReaderURL";
    public static final String PARAM_BACKEND_WRITER_URL = "backendWriterURL";
    public static final String PARAM_INIT_JOURNAL = "initJournal";
    private static final String PARAM_BLOB_CACHE_DIR = "blobCacheDir";

    private String journalId;
    private String backendReaderURL;
    private String backendWriterURL;
    private String initJournal;
    private String blobCacheDir;

    public SimpleNodeStoreBuilder() {
       journalId = "golden";
       initJournal = null;
       backendReaderURL = "tcp://localhost:8000";
       backendWriterURL = "tcp://localhost:8001";
       blobCacheDir = "/tmp/blobCacheDir";
    }

    public SimpleNodeStoreBuilder initFromEnvironment() {
        try {
            journalId = System.getenv(PARAM_JOURNAL_ID);
        } catch (Exception e) {
            // ignore
        }
        if (journalId == null) {
            journalId = "golden";
        }
        try {
            initJournal = System.getenv(PARAM_INIT_JOURNAL);
        } catch (Exception e) {
            // ignore
        }
        try {
            backendReaderURL = System.getenv(PARAM_BACKEND_READER_URL);
        } catch (Exception e) {
            // ignore
        }
        if (backendReaderURL == null) {
            backendReaderURL = "tcp://localhost:8001";
        }
        try {
            backendWriterURL = System.getenv(PARAM_BACKEND_WRITER_URL);
        } catch (Exception e) {
            // ignore
        }
        if (backendWriterURL == null) {
            backendWriterURL = "tcp://localhost:8000";
        }
        try {
            blobCacheDir = System.getenv(PARAM_BLOB_CACHE_DIR);
        } catch (Exception e) {
            // ignore
        }
        if (blobCacheDir == null) {
            blobCacheDir = "/tmp/blobCacheDir";
        }
        return this;
    }

    // simple://<journalid>?param=value...
    public SimpleNodeStoreBuilder initFromURI(URI uri) {
        if (!"simple".equals(uri.getScheme())) {
            throw new IllegalArgumentException("Expected protocol is 'simple' but I got " + uri.getScheme());
        }
        if (uri.getPort() != -1) {
            throw new IllegalArgumentException("Unexpected port setting in URL");
        }
        setJournalId(uri.getHost());
        final Map<String, String> params = new HashMap<>();
        try {
            String query = uri.getQuery();
            if (query != null) {
                for (String kv : query.split("&")) {
                    String[] aKV = kv.split("=");
                    params.put(URLDecoder.decode(aKV[0], "UTF-8"), URLDecoder.decode(aKV[1], "UTF-8"));
                }
            }
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        if (params.containsKey(PARAM_INIT_JOURNAL)) {
            try {
                setInitJournal(params.get(PARAM_INIT_JOURNAL));
            } catch (Exception e) {
                log.warn(e.getMessage());
                throw new IllegalArgumentException(e);
            }
        }
        if (params.containsKey(PARAM_BACKEND_READER_URL)) {
            try {
                setBackendReaderURL(params.get(PARAM_BACKEND_READER_URL));
            } catch (Exception e) {
                log.warn(e.getMessage());
                throw new IllegalArgumentException(e);
            }
        }
        if (params.containsKey(PARAM_BACKEND_WRITER_URL)) {
            try {
                setBackendWriterURL(params.get(PARAM_BACKEND_WRITER_URL));
            } catch (Exception e) {
                log.warn(e.getMessage());
                throw new IllegalArgumentException(e);
            }
        }
        if (params.containsKey(PARAM_BLOB_CACHE_DIR)) {
            try {
                setBlobCacheDir(params.get(PARAM_BLOB_CACHE_DIR));
            } catch (Exception e) {
                log.warn(e.getMessage());
                throw new IllegalArgumentException(e);
            }
        }
        return this;
    }

    public String getJournalId() {
       return journalId;
    }

    public SimpleNodeStoreBuilder setJournalId(String journalId) {
       this.journalId = journalId;
       return this;
    }

    public String getInitJournal() {
        return initJournal;
    }

    public SimpleNodeStoreBuilder setInitJournal(String initJournal) {
        this.initJournal = initJournal;
        return this;
    }

    public String getBackendReaderURL() {
        return backendReaderURL;
    }

    public String getBackendWriterURL() {
        return backendWriterURL;
    }

    public SimpleNodeStoreBuilder setBackendReaderURL(String backendReaderURL) {
        this.backendReaderURL = backendReaderURL;
        return this;
    }

    public SimpleNodeStoreBuilder setBackendWriterURL(String backendWriterURL) {
        this.backendWriterURL = backendWriterURL;
        return this;
    }

    public String getBlobCacheDir() {
       return blobCacheDir;
    }

    public SimpleNodeStoreBuilder setBlobCacheDir(String blobCacheDir) {
       this.blobCacheDir = blobCacheDir;
       return this;
    }

    public SimpleNodeStore build() {
       final SimpleNodeStore ret = new SimpleNodeStore(this);
       ret.init();
       return ret;
    }
}
