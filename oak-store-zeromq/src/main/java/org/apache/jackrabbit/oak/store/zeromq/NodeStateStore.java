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

import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.io.IOException;

public interface NodeStateStore {
    /**
     * Get a nodestate by reference.
     * @param ref uniquely identifies the desired nodestate.
     * @return the serialised representation of the nodestate in an implementation-dependent format.
     * @throws java.io.FileNotFoundException if the nodestate does not exist in the store.
     * @throws IOException if anything else goes wrong.
     */
    String getNodeState(String ref) throws IOException;

    /**
     * Check if a nodestate already exists in the store.
     * @param ref uniquely identifies the desired nodestate.
     * @return true if the nodestate exists, false otherwise.
     */
    boolean hasNodeState(String ref) throws IOException;

    /**
     * Save a nodestate in the store. The nodestate is serialised into an implementation-dependent format
     * and stored that way.
     * @param ns the nodestate to be saved.
     * @return the reference which uniquely identifies the stored nodestate.
     */
    SimpleNodeState putNodeState(NodeState ns) throws IOException;
}
