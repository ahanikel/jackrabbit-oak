package org.apache.jackrabbit.oak.store.zeromq;

import org.zeromq.ZMQ;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class ZeroMQSocketProvider implements Supplier<ZMQ.Socket> {

    private final String url;
    private final ZMQ.Context context;
    private final int socketType;
    private final Map<Long, ZMQ.Socket> sockets = new ConcurrentHashMap<Long, ZMQ.Socket>();

    public ZeroMQSocketProvider(String url, ZMQ.Context context, int socketType) {
        this.url = url;
        this.context = context;
        this.socketType = socketType;
    }

    @Override
    public ZMQ.Socket get() {
        long threadId = Thread.currentThread().getId();
        ZMQ.Socket socket = sockets.getOrDefault(threadId, null);
        if (socket == null) {
            socket = context.socket(socketType);
            socket.connect(url);
            sockets.put(threadId, socket);
        }
        return socket;
    }
}
