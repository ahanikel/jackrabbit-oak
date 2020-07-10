package org.apache.jackrabbit.oak.store.zeromq;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.Preconditions.*;

public class ZeroMQBuilder implements NodeBuilder {
    private String name;
    private final ZeroMQNodeStore ns;
    private ZeroMQBuilder parent;
    private ZeroMQNodeState baseState;
    private final Function<String, ZeroMQNodeState> reader;
    private final Consumer<ZeroMQNodeState.SerialisedZeroMQNodeState> writer;

    private final Map<String,ZeroMQBuilder> childrenAdded = new HashMap<>();
    private final Map<String,ZeroMQBuilder> childrenChanged = new HashMap<>();
    private final List<String> childrenRemoved = new ArrayList<>();

    private final Map<String,ZeroMQPropertyState> propertiesAdded = new HashMap<>();
    private final Map<String,ZeroMQPropertyState> propertiesChanged = new HashMap<>();
    private final List<String> propertiesRemoved = new ArrayList<>();
    private final Map<String, ZeroMQBuilder> builders = new HashMap<>();

    private boolean dirty = false;

    ZeroMQBuilder(String name, ZeroMQNodeStore ns, ZeroMQBuilder parent, ZeroMQNodeState baseState, Function<String, ZeroMQNodeState> reader, Consumer<ZeroMQNodeState.SerialisedZeroMQNodeState> writer) {
        this.name = name;
        this.ns = ns;
        this.parent = parent;
        this.baseState = baseState;
        this.reader = reader;
        this.writer = writer;
    }

    ZeroMQBuilder getChildBuilder(String name, ZeroMQNodeState baseState) {
        ZeroMQBuilder child;
        if (builders.containsKey(name)) {
            child = builders.get(name);
            child.baseState = baseState; // TODO: reset?
            child.parent = this;
        } else {
            child = new ZeroMQBuilder(name, ns, this, baseState, reader, writer);
            builders.put(name, child);
        }
        return child;
    }

    @Override
    public @NotNull NodeState getNodeState() {
        checkBuilderDeleted();
        if (!dirty) {
            return baseState;
        }

        final Map<@NotNull String, @NotNull String> children = new HashMap<>();
        baseState.getChildNodeEntries().forEach(e -> children.put(e.getName(), ((ZeroMQNodeState) e.getNodeState()).getUuid()));
        childrenRemoved.forEach(children::remove);
        childrenChanged.forEach((k, v) -> children.put(k, ((ZeroMQNodeState) v.getNodeState()).getUuid()));
        childrenAdded.forEach((k,v ) -> children.put(k, ((ZeroMQNodeState) v.getNodeState()).getUuid()));

        final Map<String, ZeroMQPropertyState> properties = new HashMap<>();
        baseState.getProperties().forEach(p -> properties.put(p.getName(), (ZeroMQPropertyState) p));
        propertiesRemoved.forEach(properties::remove);
        propertiesChanged.forEach(properties::put);
        propertiesAdded.forEach(properties::put);

        final ZeroMQNodeState ret = new ZeroMQNodeState(ns, children, properties, null, reader, writer);
        return ret;
    }

    @Override
    public @NotNull NodeState getBaseState() {
        checkBuilderDeleted();
        return baseState;
    }

    @Override
    public boolean exists() {
        return baseState != null && baseState.exists();
    }

    @Override
    public boolean isNew() {
        if (baseState == null) {
            return true;
        }
        if (parent == null) {
            return false;
        }
        return parent.isNew()
                || parent.childrenAdded.containsKey(name)
                || (parent.baseState != null && !parent.baseState.hasChildNode(name));
    }

    @Override
    public boolean isNew(String name) {
        return propertiesAdded.containsKey(name);
    }

    @Override
    public boolean isModified() {
        return dirty;
    }

    @Override
    public boolean isReplaced() {
        return false;
    }

    @Override
    public boolean isReplaced(String name) {
        checkBuilderDeleted();
        return baseState.exists();
    }

    @Override
    public long getChildNodeCount(long max) {
        checkBuilderDeleted();
        return baseState.getChildNodeCount(max) - childrenRemoved.size() + childrenAdded.size();
    }

    @Override
    public @NotNull Iterable<String> getChildNodeNames() {
        checkBuilderDeleted();
        return new Iterable<String>() {
            @NotNull
            @Override
            public Iterator<String> iterator() {
                final Iterator<String> baseChildren = baseState.getChildNodeNames().iterator();
                final Iterator<String> added = childrenAdded.keySet().iterator();
                return new Iterator<String>() {
                    private String next = null;

                    @Override
                    public boolean hasNext() {
                        if (next != null) {
                            return true;
                        }
                        while (baseChildren.hasNext()) {
                            next = baseChildren.next();
                            if (!childrenRemoved.contains(next)) {
                                return true;
                            }
                            next = null;
                        }
                        if (added.hasNext()) {
                            next = added.next();
                            return true;
                        }
                        return false;
                    }

                    @Override
                    public String next() {
                        if (hasNext()) {
                            final String ret = next;
                            next = null;
                            return ret;
                        }
                        return null;
                    }
                };
            }
        };
    }

    @Override
    public boolean hasChildNode(@NotNull String name) {
        checkBuilderDeleted();
        return childrenAdded.containsKey(name) || (baseState.hasChildNode(name) && !childrenRemoved.contains(name));
    }

    @Override
    public @NotNull NodeBuilder child(@NotNull String name) throws IllegalArgumentException {
        validateName(name);
        checkBuilderDeleted();
        dirty = true;
        if (childrenRemoved.contains(name)) {
            childrenRemoved.remove(name);
        }
        if (childrenChanged.containsKey(name)) {
            return childrenChanged.get(name);
        }
        if (childrenAdded.containsKey(name)) {
            return childrenAdded.get(name);
        }
        if (baseState.hasChildNode(name)) {
            final ZeroMQBuilder child = getChildBuilder(name, (ZeroMQNodeState) baseState.getChildNode(name));
            childrenChanged.put(name, child);
            return child;
        }
        final ZeroMQNodeState childBase = ZeroMQEmptyNodeState.EMPTY_NODE(ns, reader, writer);
        final ZeroMQBuilder child = getChildBuilder(name, childBase);
        childrenAdded.put(name, child);
        return child;
    }

    @Override
    public @NotNull NodeBuilder getChildNode(@NotNull String name) throws IllegalArgumentException {
        validateName(name);
        checkBuilderDeleted();
        if (childrenRemoved.contains(name)) {
            final ZeroMQNodeState childBase = ZeroMQEmptyNodeState.MISSING_NODE(ns, reader, writer);
            final ZeroMQBuilder child = getChildBuilder(name, childBase);
            return child;
        }
        if (childrenChanged.containsKey(name)) {
            return childrenChanged.get(name);
        }
        if (childrenAdded.containsKey(name)) {
            return childrenAdded.get(name);
        }
        if (baseState.hasChildNode(name)) {
            final ZeroMQBuilder child = getChildBuilder(name, (ZeroMQNodeState) baseState.getChildNode(name));
            childrenChanged.put(name, child);
            dirty = true;
            return child;
        }
        final ZeroMQNodeState childBase = ZeroMQEmptyNodeState.MISSING_NODE(ns, reader, writer);
        final ZeroMQBuilder child = getChildBuilder(name, childBase);
        return child;
    }

    @Override
    public @NotNull NodeBuilder setChildNode(@NotNull String name) throws IllegalArgumentException {
        validateName(name);
        checkBuilderDeleted();
        dirty = true;
        if (childrenRemoved.contains(name)) {
            childrenRemoved.remove(name);
        }
        if (childrenChanged.containsKey(name)) {
            childrenChanged.remove(name);
        }
        if (childrenAdded.containsKey(name)) {
            childrenAdded.remove(name);
        }
        if (baseState.hasChildNode(name)) {
            final ZeroMQBuilder child = getChildBuilder(name, (ZeroMQNodeState) baseState.getChildNode(name));
            childrenChanged.put(name, child);
            return child;
        }
        final ZeroMQNodeState childBase = ZeroMQEmptyNodeState.EMPTY_NODE(ns, reader, writer);
        final ZeroMQBuilder child = getChildBuilder(name, childBase);
        childrenAdded.put(name, child);
        return child;
    }

    void setChildBuilder(String name, ZeroMQBuilder builder) {
        validateName(name);
        checkBuilderDeleted();
        dirty = true;
        if (childrenRemoved.contains(name)) {
            childrenRemoved.remove(name);
        }
        if (childrenChanged.containsKey(name)) {
            childrenChanged.remove(name);
        }
        if (childrenAdded.containsKey(name)) {
            childrenAdded.remove(name);
        }
        if (baseState.hasChildNode(name)) {
            childrenChanged.put(name, builder);
        } else {
            childrenAdded.put(name, builder);
        }
    }

    @Override
    public @NotNull NodeBuilder setChildNode(@NotNull String name, @NotNull NodeState nodeState) throws IllegalArgumentException {
        validateName(name);
        checkBuilderDeleted();
        dirty = true;
        ZeroMQNodeState zmqNodeState = ZeroMQNodeState.fromNodeState(ns, nodeState, reader, writer);
        if (childrenRemoved.contains(name)) {
            childrenRemoved.remove(name);
        }
        if (childrenChanged.containsKey(name)) {
            childrenChanged.remove(name);
        }
        if (childrenAdded.containsKey(name)) {
            childrenAdded.remove(name);
        }
        final ZeroMQBuilder child = getChildBuilder(name, zmqNodeState);
        if (baseState.hasChildNode(name)) {
            if (nodeState.exists()) {
                childrenChanged.put(name, child);
            } else {
                childrenRemoved.add(name);
            }
        } else if (nodeState.exists()) {
            childrenAdded.put(name, child);
        }
        return child;
    }

    @Override
    public boolean remove() {
        checkBuilderDeleted();
        if (parent == null) {
            return false;
        }
        // we assume setting a missing child node is the same as deleting it
        parent.setChildNode(name, ZeroMQEmptyNodeState.MISSING_NODE(ns, reader, writer));
        parent = null;
        baseState = null;
        return true;
    }

    @Override
    public boolean moveTo(@NotNull NodeBuilder newParent, @NotNull String newName) throws IllegalArgumentException {
        validateName(newName);
        if (!exists()) {
            return false;
        }
        if (!newParent.exists()) {
            return false;
        }
        if (newParent.hasChildNode(newName)) {
            return false;
        }
        final NodeState nodeState = getNodeState();
        newParent.setChildNode(newName, nodeState);
        remove();
        return true;
    }

    @Override
    public long getPropertyCount() {
        checkBuilderDeleted();
        return baseState.getPropertyCount() - propertiesRemoved.size() + propertiesAdded.size();
    }

    @Override
    public @NotNull Iterable<? extends PropertyState> getProperties() {
        checkBuilderDeleted();
        return new Iterable<PropertyState>() {
            @NotNull
            @Override
            public Iterator<PropertyState> iterator() {
                final Iterator<? extends PropertyState> baseProperties = baseState.getProperties().iterator();
                final Iterator<ZeroMQPropertyState> added = propertiesAdded.values().iterator();
                return new Iterator<PropertyState>() {
                    private PropertyState next = null;

                    @Override
                    public boolean hasNext() {
                        if (next != null) {
                            return true;
                        }
                        while (baseProperties.hasNext()) {
                            next = baseProperties.next();
                            if (!propertiesRemoved.contains(next.getName())) {
                                return true;
                            }
                            next = null;
                        }
                        if (added.hasNext()) {
                            next = added.next();
                            return true;
                        }
                        return false;
                    }

                    @Override
                    public PropertyState next() {
                        if (hasNext()) {
                            final PropertyState ret = next;
                            next = null;
                            return ret;
                        }
                        return null;
                    }
                };
            }
        };
    }

    @Override
    public boolean hasProperty(String name) {
        return getProperty(name) != null;
    }

    @Override
    public @Nullable PropertyState getProperty(String name) {
        PropertyState ret = null;
        for (PropertyState p : getProperties()) {
            if (p.getName().equals(name)) {
                ret = p;
                break;
            }
        }
        return ret;
    }

    @Override
    public boolean getBoolean(@NotNull String name) {
        PropertyState property = getProperty(name);
        return property != null
                && property.getType() == Type.BOOLEAN
                && property.getValue(Type.BOOLEAN);
    }

    @Override
    public @Nullable String getString(String name) {
        PropertyState property = getProperty(name);
        if (property != null && property.getType() == Type.STRING) {
            return property.getValue(Type.STRING);
        } else {
            return null;
        }
    }

    @Override
    public @Nullable String getName(@NotNull String name) {
        PropertyState property = getProperty(name);
        if (property != null && property.getType() == Type.NAME) {
            return property.getValue(Type.NAME);
        } else {
            return null;
        }
    }

    @Override
    public @NotNull Iterable<String> getNames(@NotNull String name) {
        PropertyState property = getProperty(name);
        if (property != null && property.getType() == Type.NAMES) {
            return property.getValue(Type.NAMES);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public @NotNull NodeBuilder setProperty(@NotNull PropertyState property) throws IllegalArgumentException {
        final String name = property.getName();
        validateName(name);
        checkBuilderDeleted();
        dirty = true;
        if (propertiesRemoved.contains(name)) {
            propertiesRemoved.remove(name);
        }
        if (propertiesChanged.containsKey(name)) {
            propertiesChanged.remove(name);
        }
        if (propertiesAdded.containsKey(name)) {
            propertiesAdded.remove(name);
        }
        final ZeroMQPropertyState zmqProperty = ZeroMQPropertyState.fromPropertyState(ns, property);
        if (baseState.hasProperty(name)) {
            propertiesChanged.put(name, zmqProperty);
        } else {
            propertiesAdded.put(name, zmqProperty);
        }
        return this;
    }

    @Override
    public @NotNull <T> NodeBuilder setProperty(String name, @NotNull T value) throws IllegalArgumentException {
        return setProperty(ZeroMQPropertyState.fromValue(ns, name, value));
    }

    @Override
    public @NotNull <T> NodeBuilder setProperty(String name, @NotNull T value, Type<T> type) throws IllegalArgumentException {
        return setProperty(ZeroMQPropertyState.fromValue(ns, name, type, value));
    }

    @Override
    public @NotNull NodeBuilder removeProperty(String name) {
        checkBuilderDeleted();
        dirty = true;
        if (propertiesRemoved.contains(name)) {
            propertiesRemoved.remove(name);
        }
        if (propertiesChanged.containsKey(name)) {
            propertiesChanged.remove(name);
        }
        if (propertiesAdded.containsKey(name)) {
            propertiesAdded.remove(name);
        }
        if (baseState.hasProperty(name)) {
            propertiesRemoved.add(name);
        }
        return this;
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return ns.createBlob(stream);
    }

    /**
     * Throws away all changes in this builder and resets the base to the
     * given node state.
     *
     * @param newBase new base state
     */
    public void reset(@NotNull NodeState newBase) {
        dirty = false;
        childrenAdded.clear();
        childrenChanged.clear();
        childrenRemoved.clear();
        propertiesAdded.clear();
        propertiesChanged.clear();
        propertiesRemoved.clear();
        baseState = (ZeroMQNodeState) newBase;
        builders.forEach((n, b) -> {
            if (newBase.hasChildNode(n)) {
                b.reset(newBase.getChildNode(n));
            } else {
                //builders.remove(n);
            }
        });
    }

    public NodeState rebase(ZeroMQNodeState newBase) {
        NodeState head = getNodeState();
        NodeState base = getBaseState();
        if (base != newBase) {
            reset(newBase);
            head.compareAgainstBaseState(base, new ConflictAnnotatingRebaseDiff(this));
            head = getNodeState();
        }
        return head;
    }

    private void validateName(@NotNull String name) throws IllegalArgumentException {
        if (name.isEmpty()) {
            throw new IllegalArgumentException("Name must not be empty");
        }
        if (name.contains("/")) {
            throw new IllegalArgumentException("Name must not contain a forward slash");
        }
    }

    private void checkBuilderDeleted() {
        if (baseState == null) {
            throw new IllegalStateException("Builder was deleted");
        }
    }
}
