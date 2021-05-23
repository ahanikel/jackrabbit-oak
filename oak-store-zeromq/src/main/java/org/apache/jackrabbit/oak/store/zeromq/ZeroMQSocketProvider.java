package org.apache.jackrabbit.oak.store.zeromq;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.util.function.Supplier;

public class ZeroMQSocketProvider implements Supplier<Socket>, Closeable {

    private final String hostName;
    private final int port;

    public ZeroMQSocketProvider(String hostName, int port) {
        this.hostName = hostName;
        this.port = port;
    }

    @Override
    public Socket get() {
        try {
            return new Socket(hostName, port);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void close() {
    }
}
