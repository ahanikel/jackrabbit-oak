/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.segment.tool;

import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

final class PrintingDiff implements NodeStateDiff {

    private final PrintWriter pw;

    private final String path;

    private final boolean skipProps;

    PrintingDiff(final PrintWriter pw, final String path) {
        this(pw, path, false);
    }

    private PrintingDiff(final PrintWriter pw, final String path, final boolean skipProps) {
        this.pw = pw;
        this.path = path;
        this.skipProps = skipProps;
    }

    @Override
    public boolean propertyAdded(final PropertyState after) {
        if (!skipProps) {
            pw.println("p+ " + toString(after));
        }
        return true;
    }

    @Override
    public boolean propertyChanged(final PropertyState before, final PropertyState after) {
        if (!skipProps) {
            pw.println("p- " + toString(before));
            pw.println("p+ " + toString(after));
        }
        return true;
    }

    @Override
    public boolean propertyDeleted(final PropertyState before) {
        if (!skipProps) {
            pw.println("p- " + toString(before));
        }
        return true;
    }

    @Override
    public boolean childNodeAdded(final String name, final NodeState after) {
        final String p = concat(path, name);
        pw.println("n+ " + urlEncode(p));
        return after.compareAgainstBaseState(EMPTY_NODE, new PrintingDiff(
                pw, p));
    }

    @Override
    public boolean childNodeChanged(final String name, final NodeState before, final NodeState after) {
        final String p = concat(path, name);
        pw.println("n^ " + urlEncode(p));
        return after.compareAgainstBaseState(before,
                new PrintingDiff(pw, p));
    }

    @Override
    public boolean childNodeDeleted(final String name, final NodeState before) {
        final String p = concat(path, name);
        pw.println("n- " + urlEncode(p));
        return MISSING_NODE.compareAgainstBaseState(before, new PrintingDiff(pw, p, true));
    }

    private static String toString(final PropertyState ps) {
        final StringBuilder val = new StringBuilder();
        val.append(urlEncode(ps.getName()));
        val.append(" <");
        val.append(ps.getType());
        val.append("> ");
        if (ps.getType() == BINARY) {
            final String v = ps.getValue(BINARY).getReference();
            val.append("= ").append(v);
        }
        else if (ps.getType() == BINARIES) {
            val.append("= [");
            ps.getValue(BINARIES).forEach((Blob b) -> {
                val.append(b.getReference());
                val.append(',');
            });
            replaceOrAppendLastChar(val, ',', ']');
        }
        else if (ps.isArray()) {
            val.append("= [");
            ps.getValue(STRINGS).forEach((String s) -> {
                val.append(urlEncode(s));
                val.append(',');
            });
            replaceOrAppendLastChar(val, ',', ']');
        }
        else {
            val.append("= ").append(urlEncode(ps.getValue(STRING)));
        }
        return val.toString();
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

    private static void replaceOrAppendLastChar(StringBuilder b, char oldChar, char newChar) {
        if (b.charAt(b.length() - 1) == oldChar) {
            b.setCharAt(b.length() - 1, newChar);
        }
        else {
            b.append(newChar);
        }
    }
}
