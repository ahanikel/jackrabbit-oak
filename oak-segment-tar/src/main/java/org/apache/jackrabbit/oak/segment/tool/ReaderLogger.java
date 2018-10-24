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

package org.apache.jackrabbit.oak.segment.tool;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ReaderLogger {
    public static final String LOGFILENAME_PROPERTY = ReaderLogger.class.getName() + ".filename";
    public static final String LOGFILENAME_DEFAULT = "/tmp/readerlogger.log";
    public static final int THREADWAITTIMEMILLIS = 1000;

    private final Queue<String> queue;
    private final Thread writerThread;
    private OutputStream log;
    private volatile boolean isOperational;

    private ReaderLogger() {
        isOperational = true;
        queue = new ConcurrentLinkedQueue();
        try {
            String logFileName = System.getProperty(LOGFILENAME_PROPERTY);
            if (logFileName == null || "".equals(logFileName)) {
                logFileName = LOGFILENAME_DEFAULT;
            }
            log = new FileOutputStream(logFileName);
        } catch (FileNotFoundException ex) {
            isOperational = false;
        }
        writerThread = new Thread("ReaderLogger writer") {
            @Override
            public void run() {
                for (;;) {
                    try {
                        Thread.sleep(THREADWAITTIMEMILLIS);
                    } catch (InterruptedException ex) {
                        return;
                    }
                    String entry;
                    while ((entry = queue.poll()) != null) {
                        try {
                            log.write(entry.getBytes("UTF-8"));
                        } catch (UnsupportedEncodingException ex) {
                            isOperational = false;
                        } catch (IOException ex) {
                            isOperational = false;
                        }
                    }
                }
            }
        };
    }

    public static ReaderLogger newReaderLogger() {
        final ReaderLogger r = new ReaderLogger();
        r.writerThread.start();
        return r;
    }

    public void nodeRead(String n) {
        if (n == null) {
            n = "::: unknown :::";
        }
        queueWriteLine("n? " + urlEncode(n));
    }

    public void propertyRead(String n, String p) {
        if (n == null) {
            n = "::: unknown :::";
        }
        queueWriteLine("p? " + urlEncode(n) + " " + urlEncode(p));
    }

    private void queueWriteLine(String s) {
        queue.add(System.currentTimeMillis() + " " + urlEncode(Thread.currentThread().getName()) + " " + s + "\n");
    }

    private static String urlEncode(String s) {
        String ret;
        try {
            ret = URLEncoder.encode(s, "UTF-8").replace("%2F", "/").replace("%3A", ":");
        }
        catch (UnsupportedEncodingException ex) {
            ret = "ERROR: " + ex.toString();
        }
        return ret;
    }
}
