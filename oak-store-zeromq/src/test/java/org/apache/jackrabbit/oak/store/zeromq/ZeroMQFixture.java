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

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import static org.junit.Assert.assertEquals;

public class ZeroMQFixture extends NodeStoreFixture {

    final static ZeroMQJournal journal = ZeroMQJournal.newZeroMQJournal();

    final static ZeroMQBackendStore store = new ZeroMQBackendStore();

    final static ZeroMQBackendBlob blobStore = new ZeroMQBackendBlob();

    final static ZeroMQNodeStore ns = new ZeroMQNodeStore();

    static volatile boolean isInitialized = false;

    @Override
    public NodeStore createNodeStore() {
        synchronized(ns) {
            if (!isInitialized) {
                store.open();
                blobStore.open();
                ns.init();
                ns.reset();
                isInitialized = true;
            }
        }
        return ns;
    }

    @Override
    public void dispose(NodeStore nodeStore) {
        super.dispose(nodeStore);
        assertEquals(ns, nodeStore);
        ns.reset();
    }
}
