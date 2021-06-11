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

/**
 * A store used for in-memory operations.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            usage();
        }
        switch (args[0]) {
            case "log": {
                if (args.length != 2) {
                    usage();
                }
                final String logFile = args[1];
                final BackendStore backendStore = new LogBackendStore(logFile, 0);
                while (true) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                break;
            }
            case "EventSink": {
                if (args.length != 3) {
                    usage();
                }
                final Runnable eventSink = new EventSink(args[1], args[2]);
                eventSink.run();
                break;
            }
            case "LogFileWriter": {
                if (args.length != 3) {
                    usage();
                }
                final Runnable logFileWriter = new LogFileWriter(args[1], args[2]);
                logFileWriter.run();
                break;
            }
            case "LogFilePublisher": {
                if (args.length != 3) {
                    usage();
                }
                final Runnable logFilePublisher = new LogFilePublisher(args[1], args[2]);
                logFilePublisher.run();
                break;
            }
            case "NodeStateAggregator": {
                if (args.length != 3) {
                    usage();
                }
                final Runnable nodeStateAggregator = new ZeroMQNodeStateAggregator(args[1], args[2]);
                nodeStateAggregator.run();
                break;
            }
        }
    }

    public static void usage() {
        System.err.println("       java -jar ... log                 <instanceName> <logFileName>");
        System.err.println("       java -jar ... EventSink           <bindIn>       <bindOut>");
        System.err.println("       java -jar ... LogFileWriter       <connectIn>    <logFileName>");
        System.err.println("       java -jar ... LogFilePublisher    <connectOut>   <logFileName>");
        System.err.println("       java -jar ... NodeStateAggregator <connectIn>    <bindOut>");
        System.exit(1);
    }
}
