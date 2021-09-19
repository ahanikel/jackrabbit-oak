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

import org.apache.jackrabbit.oak.api.Blob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;

public abstract class AbstractNodeStateAggregator implements NodeStateAggregator {

    private static final Logger log = LoggerFactory.getLogger(AbstractNodeStateAggregator.class);

    protected RecordHandler recordHandler;
    protected volatile boolean caughtup;
    protected File blobCacheDir;
    protected volatile boolean shutDown;

    public AbstractNodeStateAggregator(File blobCacheDir) {
        this.blobCacheDir = blobCacheDir;
        blobCacheDir.mkdirs();
        shutDown = false;
    }

    @Override
    public boolean hasCaughtUp() {
        return caughtup;
    }

    @Override
    public String getJournalHead(String instanceName) {
        final String ret = recordHandler.getJournalHead(instanceName);
        if (ret == null) {
            return ZeroMQEmptyNodeState.UUID_NULL.toString();
        }
        return ret;
    }

    @Override
    public String readNodeState(String msg) {
        return recordHandler.readNodeState(msg);
    }

    @Override
    public Blob getBlob(String reference) {
        return recordHandler.getBlob(reference);
    }

    @Override
    public void close() {
        shutDown = true;
    }

    public void setBlobCacheDir(String blobCacheDir) {
        this.blobCacheDir = new File(blobCacheDir);
        this.blobCacheDir.mkdirs();
    }
}
