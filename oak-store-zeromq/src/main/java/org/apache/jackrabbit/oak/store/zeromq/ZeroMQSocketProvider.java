package org.apache.jackrabbit.oak.store.zeromq;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class ZeroMQSocketProvider implements Supplier<ZMQ.Socket>, Closeable {

    private final String url;
    private final ZContext context;
    private final SocketType socketType;
    private final Map<Long, ZMQ.Socket> sockets = new ConcurrentHashMap<Long, ZMQ.Socket>();

    public ZeroMQSocketProvider(String url, ZContext context, SocketType socketType) {
        this.url = url;
        this.context = context;
        this.socketType = socketType;
    }

    @Override
    public ZMQ.Socket get() {
        long threadId = Thread.currentThread().getId();
        ZMQ.Socket socket = sockets.get(threadId);
        if (socket == null) {
            synchronized (sockets) {
                socket = sockets.get(threadId);
                if (socket == null) {
                    socket = context.createSocket(socketType);
                    socket.connect(url);
                    sockets.put(threadId, socket);
                }
            }
        }
        return socket;
    }

    @Override
    public void close() {
        for (ZMQ.Socket socket : sockets.values()) {
            socket.close();
        }
    }
}
