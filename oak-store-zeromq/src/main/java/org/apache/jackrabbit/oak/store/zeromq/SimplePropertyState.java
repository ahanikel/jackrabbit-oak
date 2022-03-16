package org.apache.jackrabbit.oak.store.zeromq;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.jetbrains.annotations.NotNull;

public class SimplePropertyState implements PropertyState {

    private final String name;
    private final Type type;
    private final String value;

    public static SimplePropertyState deserialise(String s) {
        String name = "";
        Type<?> type = Type.BOOLEAN;
        String value = "";
        return new SimplePropertyState(name, type, value);
    }

    private SimplePropertyState(String name, Type type, String value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    @Override
    public @NotNull String getName() {
        return null;
    }

    @Override
    public boolean isArray() {
        return false;
    }

    @Override
    public Type<?> getType() {
        return null;
    }

    @Override
    public <T> @NotNull T getValue(Type<T> type) {
        return null;
    }

    @Override
    public <T> @NotNull T getValue(Type<T> type, int index) {
        return null;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public long size(int index) {
        return 0;
    }

    @Override
    public int count() {
        return 0;
    }
}
