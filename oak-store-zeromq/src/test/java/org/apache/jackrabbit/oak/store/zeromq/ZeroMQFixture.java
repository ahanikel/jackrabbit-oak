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

    private volatile ZeroMQNodeStore store = null;

    public ZeroMQFixture() {
        store = new ZeroMQNodeStore();
        store.init();
    }

    @Override
    public NodeStore createNodeStore() {
        store.reset();
        return store;
    }

    @Override
    public void dispose(NodeStore nodeStore) {
        // it looks like dispose is never called so
        // we always return the same node store
        // ((ZeroMQNodeStore) store).close();
        // store = null;
    }
}
