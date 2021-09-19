/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.store.zeromq.dropwizard;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.jackrabbit.oak.store.zeromq.NodeStateAggregator;
import org.apache.jackrabbit.oak.store.zeromq.log.LogfileNodeStateAggregator;

import java.io.FileNotFoundException;
import java.io.IOException;

public class HttpBackendApplication extends Application<HttpBackendConfiguration> {
    private NodeStateAggregator aggregator;
    private Exception fatalException;
    private Thread aggregatorThread;

    public static void main(String [] args) throws Exception {
        new HttpBackendApplication().run(args);
    }

    @Override
    public String getName() {
        return "http-backend";
    }

    @Override
    public void initialize(Bootstrap<HttpBackendConfiguration> bootstrap) {
        try {
            aggregator = new LogfileNodeStateAggregator("/Users/axel/Documents/git/kafka-docker/quickstart-diff.log", "/tmp/backendBlobs");
            aggregatorThread = new Thread(aggregator, "NodeStateAggregator");
            aggregatorThread.setDaemon(true);
            aggregatorThread.start();
        } catch (IOException e) {
            fatalException = e;
        }
    }

    @Override
    public void run(HttpBackendConfiguration configuration, Environment environment) {
        final JournalHeadResource journalHeadResource = new JournalHeadResource(aggregator);
        environment.jersey().register(journalHeadResource);
        final NodeStateResource nodeStateResource = new NodeStateResource(aggregator);
        environment.jersey().register(nodeStateResource);
        final BlobResource blobResource = new BlobResource(aggregator);
        environment.jersey().register(blobResource);
    }
}
