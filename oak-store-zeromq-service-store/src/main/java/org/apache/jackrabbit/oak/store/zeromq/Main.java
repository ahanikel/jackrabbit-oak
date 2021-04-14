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

import org.apache.jackrabbit.oak.store.zeromq.dropwizard.HttpBackendApplication;
import org.apache.jackrabbit.oak.store.zeromq.kafka.KafkaBackendStore;
import org.apache.jackrabbit.oak.store.zeromq.log.LogBackendStore;

import java.io.FileNotFoundException;

/**
 * A store used for in-memory operations.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            usage();
        }
        switch (args[0]) {
            case "kafka": {
                if (args.length != 1) {
                    usage();
                }
                final BackendStore backendStore = new KafkaBackendStore();
                break;
            }
            case "log": {
                if (args.length != 2) {
                    usage();
                }
                final String logFile = args[1];
                final BackendStore backendStore = new LogBackendStore(logFile);
                break;
            }
            case "server": {
                HttpBackendApplication.main(args);
                break;
            }
        }
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    public static void usage() {
        System.err.println("Usage: java -jar ... kafka <instanceName>");
        System.err.println("       java -jar ... log   <instanceName> <logFileName>");
        System.exit(1);
    }
}
