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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.jmx.AbstractCheckpointMBean;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularDataSupport;
import java.util.Date;

/**
 * {@code CheckpointMBean} implementation for the {@code SimpleNodeStore}.
 */
public class SimpleCheckpointMBean extends AbstractCheckpointMBean {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final SimpleNodeStore store;

    public SimpleCheckpointMBean(SimpleNodeStore store) {
        this.store = store;
    }

    @Override
    protected void collectCheckpoints(TabularDataSupport tab) throws OpenDataException {
    }

    @Override
    public long getOldestCheckpointCreationTimestamp() {
        return 0;
    }

    private static String getDate(NodeState checkpoint, String name) {
        PropertyState p = checkpoint.getProperty(name);
        if (p == null) {
            return "NA";
        }

        return new Date(p.getValue(Type.LONG)).toString();
    }

    @Override
    public String createCheckpoint(long lifetime) {
        return null;
    }

    @Override
    public boolean releaseCheckpoint(String checkpoint) {
        log.info("Released checkpoint [{}]", checkpoint);
        return true;
    }
}
