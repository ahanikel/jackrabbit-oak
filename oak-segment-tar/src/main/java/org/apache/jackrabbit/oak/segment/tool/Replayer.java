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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class Replayer implements Runnable {

    private InputStream is;
    private boolean errorsAreFatal;
    private final ReplayWorker worker;

    public Replayer(NodeStore store) {
        this.worker = new ReplayWorker(store);
        this.errorsAreFatal = false;
    }

    public void setInputStream(InputStream is) {
        this.is = is;
    }

    public void errorsAreFatal(boolean fatal) {
        this.errorsAreFatal = fatal;
    }

    @Override
    public void run() {

        worker.start();

        final ReplayItemIterator it = new ReplayItemIterator(is, errorsAreFatal);

        while (it.hasNext()) {
            worker.add(it.next());
        }

        worker.stop();
        worker.join();
    }

    private enum Operation {

        OP_NODE_ADDED,
        OP_NODE_CHANGED,
        OP_NODE_REMOVED,
        OP_NODE_READ,
        OP_PROPERTY_ADDED,
        OP_PROPERTY_CHANGED,
        OP_PROPERTY_REMOVED,
        OP_PROPERTY_READ,
        OP_NODE_END,
    }

    private static class ReplayItem {

        private final Calendar cal;
        private final String thr;
        private final Operation op;
        private final String name;
        private final String type;
        private final String value;
        private final List<String> values;

        public ReplayItem(final Calendar cal, final String thr, final Operation op, final String name, final String type, final String value, final List<String> values) {
            this.cal = cal;
            this.thr = thr;
            this.op = op;
            this.name = name;
            this.type = type;
            this.value = value;
            this.values = values;
        }

        public Calendar getCal() {
            return cal;
        }

        public String getThread() {
            return thr;
        }

        public Operation getOp() {
            return op;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public String getValue() {
            return value;
        }

        public List<String> getValues() {
            return values;
        }
    }

    private static class ReplayItemIterator implements Iterator<ReplayItem> {

        private final BufferedReader reader;

        private ReplayItem currentItem = null;

        private final boolean errorsAreFatal;

        public ReplayItemIterator(InputStream is, boolean errorsAreFatal) {
            this.errorsAreFatal = errorsAreFatal;
            InputStreamReader isr = new InputStreamReader(is);
            reader = new BufferedReader(isr);
            readOne();
        }

        @Override
        public boolean hasNext() {
            return currentItem != null;
        }

        @Override
        public ReplayItem next() {
            ReplayItem ret = currentItem;
            readOne();
            return ret;
        }

        private void readOne() {
            try {
                final String line = reader.readLine();
                if (line == null) {
                    currentItem = null;
                } else {
                    final StringTokenizer t = new StringTokenizer(line);
                    currentItem = parseReplayItem(t, errorsAreFatal);
                    if (currentItem == null) {
                        readOne();
                    }
                }
            } catch (IOException ioe) {
                currentItem = null;
            }
        }

        private static String urlDecode(String s) {
            String ret;
            try {
                // I think it's not necessary to: ret = URLDecoder.decode(s.replace(":", "%3A").replace("/", "%2F"), "UTF-8");
                ret = URLDecoder.decode(s, "UTF-8");
            } catch (UnsupportedEncodingException ex) {
                throw new IllegalStateException(ex);
            }
            return ret;
        }

        private static List<String> urlDecodeList(String s) {
            List<String> ret = new ArrayList();
            if (!s.equals("[]")) {
                String[] items = s.substring(1, s.length() - 1).split(",");
                for (int i = 0; i < items.length; ++i) {
                    ret.add(urlDecode(items[i]));
                }
            }
            return ret;
        }

        private static ReplayItem parseReplayItem(StringTokenizer t, boolean errorsAreFatal) {

            final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            final long millis = Long.parseLong(t.nextToken());
            cal.setTimeInMillis(millis);
            final String sThread = urlDecode(t.nextToken());
            final String sOp = t.nextToken();

            switch (sOp) {
                case "n+": {
                    final Operation op = Operation.OP_NODE_ADDED;
                    final String name = urlDecode(t.nextToken());
                    return new ReplayItem(cal, sThread, op, name, null, null, null);
                }
                case "n-": {
                    final Operation op = Operation.OP_NODE_REMOVED;
                    final String name = urlDecode(t.nextToken());
                    return new ReplayItem(cal, sThread, op, name, null, null, null);
                }
                case "n^": {
                    final Operation op = Operation.OP_NODE_CHANGED;
                    final String name = urlDecode(t.nextToken());
                    return new ReplayItem(cal, sThread, op, name, null, null, null);
                }
                case "n?": {
                    final Operation op = Operation.OP_NODE_READ;
                    final String name = urlDecode(t.nextToken());
                    return new ReplayItem(cal, sThread, op, name, null, null, null);
                }
                case "p+": {
                    final Operation op = Operation.OP_PROPERTY_ADDED;
                    final String name = urlDecode(t.nextToken());
                    final String type = urlDecode(t.nextToken());
                    t.nextToken(); // skip "="
                    if (t.hasMoreTokens()) {
                        final String sValue = t.nextToken();
                        if (sValue.startsWith("[")) {
                            return new ReplayItem(cal, sThread, op, name, type, null, urlDecodeList(sValue));
                        } else {
                            return new ReplayItem(cal, sThread, op, name, type, urlDecode(sValue), null);
                        }
                    } else {
                        return new ReplayItem(cal, sThread, op, name, type, "", null);
                    }
                }
                case "p-": {
                    final Operation op = Operation.OP_PROPERTY_REMOVED;
                    final String name = urlDecode(t.nextToken());
                    return new ReplayItem(cal, sThread, op, name, null, null, null);
                }
                case "p^": {
                    final Operation op = Operation.OP_PROPERTY_CHANGED;
                    final String name = urlDecode(t.nextToken());
                    final String type = urlDecode(t.nextToken());
                    t.nextToken(); // skip "="
                    if (t.hasMoreTokens()) {
                        final String sValue = t.nextToken();
                        if (sValue.startsWith("[")) {
                            return new ReplayItem(cal, sThread, op, name, type, null, urlDecodeList(sValue));
                        } else {
                            return new ReplayItem(cal, sThread, op, name, type, urlDecode(sValue), null);
                        }
                    } else {
                        return new ReplayItem(cal, sThread, op, name, type, "", null);
                    }
                }
                case "p?": {
                    final Operation op = Operation.OP_PROPERTY_READ;
                    final String name = urlDecode(t.nextToken());
                    return new ReplayItem(cal, sThread, op, name, null, null, null);
                }
                case "n!": {
                    final Operation op = Operation.OP_NODE_END;
                    return new ReplayItem(cal, sThread, op, null, null, null, null);
                }
                default: {
                    if (errorsAreFatal) {
                        throw new IllegalStateException("Unknown operation: " + sOp);
                    }
                    return null;
                }
            }
        }
    }

    private static class HexStringInputStream extends InputStream {

        private final String hex;
        private int pos = 0;

        HexStringInputStream(final String hex) {
            this.hex = hex;
        }

        @Override
        public int read() throws IOException {
            if (pos >= hex.length()) {
                return -1;
            }
            String s = hex.substring(pos++, ++pos);
            return Integer.parseUnsignedInt(s, 16);
        }
    }

    private static class ReplayWorker implements Queue<ReplayItem> {

        private final Queue<ReplayItem> queue;
        private final Runnable runner;
        private final ExecutorService executor;
        private volatile boolean terminationRequest;

        private static final int THREADWAITTIMEMILLIS = 1;
        private static final int NWORKERS = 1;

        public ReplayWorker(final NodeStore store) {
            this.queue = new ConcurrentLinkedQueue();
            this.terminationRequest = false;
            this.executor = Executors.newFixedThreadPool(NWORKERS);

            this.runner = new Runnable() {

                @Override
                public void run() {
                    while (!terminationRequest) {
                        while (!queue.isEmpty()) {
                            NodeState root = store.getRoot();
                            NodeBuilder builder = root.builder();
                            run(builder);
                            try {
                                store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                            } catch (CommitFailedException ex) {
                                System.err.println(ex.toString());
                            }
                        }
                        try {
                            Thread.sleep(THREADWAITTIMEMILLIS);
                        } catch (InterruptedException ex) {
                            terminationRequest = true;
                        }
                    }
                }

                private void run(NodeBuilder builder) {
                    for (;;) {
                        for (ReplayItem i = queue.poll(); i != null; i = queue.poll()) {
                            switch (i.getOp()) {
                                case OP_NODE_END:
                                    return;
                                case OP_NODE_ADDED: {
                                    run(builder.child(i.getName()));
                                    break;
                                }
                                case OP_NODE_CHANGED: {
                                    run(builder.child(i.getName()));
                                    break;
                                }
                                case OP_NODE_REMOVED: {
                                    builder.child(i.getName()).remove();
                                    break;
                                }
                                case OP_NODE_READ: {
                                    NodeState cursor = store.getRoot();
                                    String[] path = i.getName().split("/");
                                    for (int idx = 1; idx < path.length; ++idx) {
                                        cursor = cursor.getChildNode(path[idx]);
                                    }
                                    break;
                                }
                                case OP_PROPERTY_ADDED: {
                                    setProperty(builder, i.getName(), i.getType(), i.getValue(), i.getValues());
                                    break;
                                }
                                case OP_PROPERTY_CHANGED: {
                                    setProperty(builder, i.getName(), i.getType(), i.getValue(), i.getValues());
                                    break;
                                }
                                case OP_PROPERTY_REMOVED: {
                                    builder.removeProperty(i.getName());
                                    break;
                                }
                                case OP_PROPERTY_READ: {
                                    NodeState cursor = store.getRoot();
                                    String[] path = i.getName().split("/");
                                    for (int idx = 1; idx < path.length - 1; ++idx) {
                                        cursor = cursor.getChildNode(path[idx]);
                                    }
                                    PropertyState p = cursor.getProperty(path[path.length - 1]);
                                    if (p != null) {
                                        String val = p.getValue(p.getType()).toString();
                                        if (Boolean.parseBoolean("true") == false) {
                                            System.err.println(val);
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                        try {
                            Thread.sleep(THREADWAITTIMEMILLIS);
                        } catch (InterruptedException ex) {
                            terminationRequest = true;
                            return;
                        }
                    }
                }
            };
        }

        private static void setProperty(NodeBuilder builder, String name, String type, String value, List<String> values) {
            switch (type) {
                case "<STRING>":
                    builder.setProperty(name, value, Type.STRING);
                    break;
                case "<STRINGS>":
                    builder.setProperty(name, values, Type.STRINGS);
                    break;
                // TODO: we should really treat blobs as non-strict
                case "<BINARY>": {
                    Blob blob;
                    try {
                        blob = builder.createBlob(new HexStringInputStream(value));
                    } catch (IOException ex) {
                        throw new IllegalStateException(ex);
                    }
                    builder.setProperty(name, blob, Type.BINARY);
                    break;
                }
                // TODO: we should really treat blobs as non-strict
                case "<BINARIES>": {
                    List<Blob> blobs = new ArrayList();
                    values.forEach(val -> {
                        Blob blob;
                        try {
                            blob = builder.createBlob(new HexStringInputStream(val));
                        } catch (IOException ex) {
                            throw new IllegalStateException(ex);
                        }
                        blobs.add(blob);
                    });
                    builder.setProperty(name, blobs, Type.BINARIES);
                    break;
                }
                case "<LONG>":
                    builder.setProperty(name, Long.valueOf(value), Type.LONG);
                    break;
                case "<LONGS>": {
                    List<Long> vals = new ArrayList();
                    values.forEach(val -> vals.add(Long.valueOf(val)));
                    builder.setProperty(name, vals, Type.LONGS);
                    break;
                }
                case "<DOUBLE>":
                    builder.setProperty(name, Double.valueOf(value), Type.DOUBLE);
                    break;
                case "<DOUBLES>": {
                    List<Double> vals = new ArrayList();
                    values.forEach(val -> vals.add(Double.valueOf(val)));
                    builder.setProperty(name, vals, Type.DOUBLES);
                    break;
                }
                case "<DATE>":
                    builder.setProperty(name, value, Type.DATE);
                    break;
                case "<DATES>":
                    builder.setProperty(name, values, Type.DATES);
                    break;
                case "<BOOLEAN>":
                    builder.setProperty(name, Boolean.valueOf(value), Type.BOOLEAN);
                    break;
                case "<BOOLEANS>": {
                    List<Boolean> vals = new ArrayList();
                    values.forEach(val -> vals.add(Boolean.valueOf(val)));
                    builder.setProperty(name, vals, Type.BOOLEANS);
                    break;
                }
                case "<NAME>":
                    builder.setProperty(name, value, Type.NAME);
                    break;
                case "<NAMES>":
                    builder.setProperty(name, values, Type.NAMES);
                    break;
                case "<PATH>":
                    builder.setProperty(name, value, Type.PATH);
                    break;
                case "<PATHS>":
                    builder.setProperty(name, values, Type.PATHS);
                    break;
                case "<REFERENCE>":
                    builder.setProperty(name, value, Type.REFERENCE);
                    break;
                case "<REFERENCES>":
                    builder.setProperty(name, values, Type.REFERENCES);
                    break;
                case "<WEAKREFERENCE>":
                    builder.setProperty(name, value, Type.WEAKREFERENCE);
                    break;
                case "<WEAKREFERENCES>":
                    builder.setProperty(name, values, Type.WEAKREFERENCES);
                    break;
                case "<URI>":
                    builder.setProperty(name, value, Type.URI);
                    break;
                case "<URIS>":
                    builder.setProperty(name, values, Type.URIS);
                    break;
                case "<DECIMAL>":
                    builder.setProperty(name, new BigDecimal(value), Type.DECIMAL);
                    break;
                case "<DECIMALS>": {
                    List<BigDecimal> vals = new ArrayList();
                    values.forEach(val -> vals.add(new BigDecimal(val)));
                    builder.setProperty(name, vals, Type.DECIMALS);
                    break;
                }
                case "<UNDEFINED>":
                    throw new IllegalStateException(); // no idea how to implement that :-)
                case "<UNDEFINEDS>":
                    throw new IllegalStateException(); // no idea how to implement that :-)
            }
        }

        public void start() {
            for (int i = 0; i < NWORKERS; ++i) {
                executor.execute(runner);
            }
        }

        public void stop() {
            terminationRequest = true;
        }

        public void join() {
            try {
                executor.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException ex) {
            }
        }

        @Override
        public boolean add(ReplayItem e) {
            return queue.add(e);
        }

        @Override
        public boolean offer(ReplayItem e) {
            return queue.offer(e);
        }

        @Override
        public ReplayItem remove() {
            return queue.remove();
        }

        @Override
        public ReplayItem poll() {
            return queue.poll();
        }

        @Override
        public ReplayItem element() {
            return queue.element();
        }

        @Override
        public ReplayItem peek() {
            return queue.peek();
        }

        @Override
        public int size() {
            return queue.size();
        }

        @Override
        public boolean isEmpty() {
            return queue.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return queue.contains(o);
        }

        @Override
        public Iterator<ReplayItem> iterator() {
            return queue.iterator();
        }

        @Override
        public Object[] toArray() {
            return queue.toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return queue.toArray(a);
        }

        @Override
        public boolean remove(Object o) {
            return queue.remove(o);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return queue.containsAll(c);
        }

        @Override
        public boolean addAll(Collection<? extends ReplayItem> c) {
            return queue.addAll(c);
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return queue.removeAll(c);
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return queue.retainAll(c);
        }

        @Override
        public void clear() {
            queue.clear();
        }
    }
}
