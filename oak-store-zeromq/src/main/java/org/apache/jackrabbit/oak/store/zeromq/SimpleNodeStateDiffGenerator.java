package org.apache.jackrabbit.oak.store.zeromq;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.jackrabbit.oak.store.zeromq.SafeEncode.safeEncode;

public class SimpleNodeStateDiffGenerator implements NodeStateDiff {

    private final Map<String, String> childrenMap;
    private final Map<String, String> propertiesMap;
    private final SimpleNodeStore store;

    public SimpleNodeStateDiffGenerator(SimpleNodeState base) {
        this.childrenMap = new HashMap<>();
        this.propertiesMap = new HashMap<>();
        this.store = base.getStore();

        childrenMap.putAll(base.getChildrenMap());
        propertiesMap.putAll(base.getPropertiesMap());
    }

    public SimpleNodeState getNodeState() throws IOException {
        final BlobStore blobStore = store.getRemoteBlobStore();

        final List<String> children = new ArrayList<>();
        for (Map.Entry<String, String> e : childrenMap.entrySet()) {
            children.add("n+ " + safeEncode(e.getKey()) + " " + e.getValue());
        }

        final List<String> properties = new ArrayList<>();
        for (String p : propertiesMap.values()) {
            properties.add("p+ " + p);
        }

        children.sort(Comparator.naturalOrder());
        properties.sort(Comparator.naturalOrder());

        final File tempFile = blobStore.getTempFile();
        try (OutputStream os = new FileOutputStream(tempFile)) {
            writeLine(os, "n:");
            for (String c : children) {
                writeLine(os, c);
            }
            for (String p : properties) {
                writeLine(os, p);
            }
            writeLine(os, "n!");
        }
        String ref;
        try {
            ref = blobStore.putTempFile(tempFile);
        } catch (BlobAlreadyExistsException e) {
            ref = e.getRef();
        }
        return SimpleNodeState.get(store, ref, childrenMap, propertiesMap);
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        try {
            String ps = serializePropertyState(store.getRemoteBlobStore(), after);
            propertiesMap.put(after.getName(), ps);
            return true;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        try {
            String ps = serializePropertyState(store.getRemoteBlobStore(), after);
            propertiesMap.replace(after.getName(), ps);
            return true;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        propertiesMap.remove(before.getName());
        return true;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        try {
            SimpleNodeState child;
            if (after instanceof SimpleNodeState) {
                child = (SimpleNodeState) after;
            } else if (after instanceof EmptyNodeState) {
                child = SimpleNodeState.empty(store);
            } else {
                SimpleNodeState before = SimpleNodeState.empty(store);
                SimpleNodeStateDiffGenerator generator = new SimpleNodeStateDiffGenerator(before);
                after.compareAgainstBaseState(before, generator);
                child = generator.getNodeState();
            }
            childrenMap.put(name, child.getRef());
            return true;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        childrenMap.remove(name);
        return childNodeAdded(name, after);
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        childrenMap.remove(name);
        return true;
    }

    private String serializePropertyState(BlobStore blobStore, PropertyState ps) throws IOException {
        final StringBuilder sb = new StringBuilder();
        sb
                .append(safeEncode(ps.getName()))
                .append(" <")
                .append(ps.getType().toString())
                .append("> ");
        if (ps.getType().equals(Type.BINARY)) {
            final Blob blob = ps.getValue(Type.BINARY);
            if (blob instanceof SimpleBlob) {
                sb.append(blob.getReference());
            } else {
                String ref;
                try {
                    ref = blobStore.putInputStream(blob.getNewStream());
                } catch (BlobAlreadyExistsException e) {
                    ref = e.getRef();
                }
                sb.append(ref);
            }
        } else if (ps.getType().equals(Type.BINARIES)) {
            sb.append('[');
            final Iterable<Blob> blobs = ps.getValue(Type.BINARIES);
            for (Blob blob : blobs) {
                if (blob instanceof SimpleBlob) {
                    sb.append(blob.getReference());
                } else {
                    String ref;
                    try {
                        ref = blobStore.putInputStream(blob.getNewStream());
                    } catch (BlobAlreadyExistsException e) {
                        ref = e.getRef();
                    }
                    sb.append(ref);
                }
                sb.append(',');
            }
            if (sb.charAt(sb.length() - 1) == ',') {
                sb.deleteCharAt(sb.length() - 1);
            }
            sb.append(']');
        } else if (ps.isArray()) {
            sb.append('[');
            final Iterable<String> strings = ps.getValue(Type.STRINGS);
            for (String s : strings) {
                sb.append(SafeEncode.safeEncode(s));
                sb.append(',');
            }
            if (sb.charAt(sb.length() - 1) == ',') {
                sb.deleteCharAt(sb.length() - 1);
            }
            sb.append(']');
        } else {
            sb.append(SafeEncode.safeEncode(ps.getValue(Type.STRING)));
        }
        return sb.toString();
    }

    private void writeLine(OutputStream os, String s) throws IOException {
        os.write(s.getBytes());
        os.write('\n');
    }
}