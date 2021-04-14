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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

public class ZeroMQNodeStoreBuilder {

    private static final Logger log = LoggerFactory.getLogger(ZeroMQNodeStoreBuilder.class.getName());

    public static final String PARAM_CLUSTERINSTANCES = "clusterInstances";
    public static final String PARAM_BACKEND_PREFIX = "backendPrefix";
    public static final String PARAM_WRITEBACKJOURNAL = "writeBackJournal";
    public static final String PARAM_WRITEBACKNODES = "writeBackNodes";
    public static final String PARAM_INITJOURNAL = "initJournal";
    public static final String PARAM_REMOTEREADS = "remoteReads";
    private static final String PARAM_LOG_NODE_STATES = "logNodeStates";
    private static final String PARAM_BLOBCACHE_DIR = "blobCacheDir";

    private String journalId;
    private int clusterInstances;
    private boolean writeBackJournal;
    private boolean writeBackNodes;
    private boolean remoteReads;
    private String initJournal;
    private String backendPrefix;
    private boolean logNodeStates;
    private String blobCacheDir;

   public ZeroMQNodeStoreBuilder() {
       journalId = "golden";
       clusterInstances = 1;
       writeBackJournal = false;
       writeBackNodes = false;
       remoteReads = true;
       initJournal = null;
       backendPrefix = "localhost";
       logNodeStates = false;
       blobCacheDir = "/tmp/blobs";
    }

    public ZeroMQNodeStoreBuilder initFromEnvironment() {
        try {
            clusterInstances = Integer.parseInt(System.getenv(PARAM_CLUSTERINSTANCES));
        } catch (Exception e) {
            // ignore
        }
        try {
            writeBackJournal = Boolean.parseBoolean(System.getenv(PARAM_WRITEBACKJOURNAL));
        } catch (Exception e) {
            // ignore
        }
        try {
            writeBackNodes = Boolean.parseBoolean(System.getenv(PARAM_WRITEBACKNODES));
        } catch (Exception e) {
            // ignore
        }
        try {
            remoteReads = Boolean.parseBoolean(System.getenv(PARAM_REMOTEREADS));
        } catch (Exception e) {
            // ignore
        }
        try {
            initJournal = System.getenv(PARAM_INITJOURNAL);
        } catch (Exception e) {
            // ignore
        }
        try {
            backendPrefix = System.getenv(PARAM_BACKEND_PREFIX);
        } catch (Exception e) {
            // ignore
        }
        try {
            logNodeStates = Boolean.parseBoolean(System.getenv(PARAM_LOG_NODE_STATES));
        } catch (Exception e) {
            // ignore
        }
        return this;
    }

    public ZeroMQNodeStoreBuilder initFromURI(URI uri) {
        if (!"zeromq".equals(uri.getScheme())) {
            throw new IllegalArgumentException("Expected protocol is 'zeromq' but I got " + uri.getScheme());
        }
        if (uri.getPort() != -1) {
            throw new IllegalArgumentException("Unexpected port setting in zeromq URL");
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
        if (params.containsKey(PARAM_CLUSTERINSTANCES)) {
            try {
                setClusterInstances(Integer.parseInt(params.get(PARAM_CLUSTERINSTANCES)));
            } catch (Exception e) {
                log.warn(e.getMessage());
                throw new IllegalArgumentException(e);
            }
        }
        if (params.containsKey(PARAM_WRITEBACKJOURNAL)) {
            try {
                setWriteBackJournal(Boolean.parseBoolean(params.get(PARAM_WRITEBACKJOURNAL)));
            } catch (Exception e) {
                log.warn(e.getMessage());
                throw new IllegalArgumentException(e);
            }
        }
        if (params.containsKey(PARAM_WRITEBACKNODES)) {
            try {
                setWriteBackNodes(Boolean.parseBoolean(params.get(PARAM_WRITEBACKNODES)));
            } catch (Exception e) {
                log.warn(e.getMessage());
                throw new IllegalArgumentException(e);
            }
        }
        if (params.containsKey(PARAM_REMOTEREADS)) {
            try {
                setRemoteReads(Boolean.parseBoolean(params.get(PARAM_REMOTEREADS)));
            } catch (Exception e) {
                log.warn(e.getMessage());
                throw new IllegalArgumentException(e);
            }
        }
        if (params.containsKey(PARAM_INITJOURNAL)) {
            try {
                setInitJournal(params.get(PARAM_INITJOURNAL));
            } catch (Exception e) {
                log.warn(e.getMessage());
                throw new IllegalArgumentException(e);
            }
        }
        if (params.containsKey(PARAM_BACKEND_PREFIX)) {
            try {
                setBackendPrefix(params.get(PARAM_BACKEND_PREFIX));
            } catch (Exception e) {
                log.warn(e.getMessage());
                throw new IllegalArgumentException(e);
            }
        }
        if (params.containsKey(PARAM_LOG_NODE_STATES)) {
            try {
                setLogNodeStates(Boolean.parseBoolean(params.get(PARAM_LOG_NODE_STATES)));
            } catch (Exception e) {
                log.warn(e.getMessage());
                throw new IllegalArgumentException(e);
            }
        }
        if (params.containsKey(PARAM_BLOBCACHE_DIR)) {
            try {
                setBlobCacheDir(params.get(PARAM_BLOBCACHE_DIR));
            } catch (Exception e) {
                log.warn(e.getMessage());
                throw new IllegalArgumentException(e);
            }
        }
        return this;
    }

    public ZeroMQNodeStoreBuilder initFromURIString(String uri) throws MalformedURLException {
       return initFromURI(URI.create(uri));
    }

    public String getJournalId() {
       return journalId;
    }

    public ZeroMQNodeStoreBuilder setJournalId(String journalId) {
       this.journalId = journalId;
       return this;
    }

    public int getClusterInstances() {
        return clusterInstances;
    }

    public ZeroMQNodeStoreBuilder setClusterInstances(int clusterInstances) {
        this.clusterInstances = clusterInstances;
        return this;
    }

    public boolean isWriteBackJournal() {
        return writeBackJournal;
    }

    public ZeroMQNodeStoreBuilder setWriteBackJournal(boolean writeBackJournal) {
        this.writeBackJournal = writeBackJournal;
        return this;
    }

    public boolean isWriteBackNodes() {
        return writeBackNodes;
    }

    public ZeroMQNodeStoreBuilder setWriteBackNodes(boolean writeBackNodes) {
        this.writeBackNodes = writeBackNodes;
        return this;
    }

    public boolean isRemoteReads() {
        return remoteReads;
    }

    public ZeroMQNodeStoreBuilder setRemoteReads(boolean remoteReads) {
        this.remoteReads = remoteReads;
        return this;
    }

    public String getInitJournal() {
        return initJournal;
    }

    public ZeroMQNodeStoreBuilder setInitJournal(String initJournal) {
        this.initJournal = initJournal;
        return this;
    }

    public String getBackendPrefix() {
        return backendPrefix;
    }

    public ZeroMQNodeStoreBuilder setBackendPrefix(String backendPrefix) {
        this.backendPrefix = backendPrefix;
        return this;
    }

    public boolean isLogNodeStates() {
        return logNodeStates;
    }

    ZeroMQNodeStoreBuilder setLogNodeStates(boolean logNodeStates) {
        this.logNodeStates = logNodeStates;
        return this;
    }

    public String getBlobCacheDir() {
       return blobCacheDir;
    }

    ZeroMQNodeStoreBuilder setBlobCacheDir(String blobCacheDir) {
       this.blobCacheDir = blobCacheDir;
       return this;
    }

    public ZeroMQNodeStore build() {
       final ZeroMQNodeStore ret = new ZeroMQNodeStore(
               getJournalId(),
               getClusterInstances(),
               isWriteBackJournal(),
               isWriteBackNodes(),
               isRemoteReads(),
               getInitJournal(),
               getBackendPrefix(),
               isLogNodeStates(),
               getBlobCacheDir()
       );
       ret.init();
       return ret;
    }
}
