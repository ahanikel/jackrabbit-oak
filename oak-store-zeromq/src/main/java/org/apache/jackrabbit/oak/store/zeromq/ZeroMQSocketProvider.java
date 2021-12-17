package org.apache.jackrabbit.oak.store.zeromq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class ZeroMQSocketProvider implements Supplier<ZeroMQSocketProvider.Socket>, Closeable {

    private static Logger log = LoggerFactory.getLogger(ZeroMQSocketProvider.class);
    private final String url;
    private final ZContext context;
    private final SocketType socketType;
    private final Map<Long, Socket> sockets = new ConcurrentHashMap<Long, Socket>();

    public ZeroMQSocketProvider(String url, ZContext context, SocketType socketType) {
        this.url = url;
        this.context = context;
        this.socketType = socketType;
    }

    @Override
    public Socket get() {
        long threadId = Thread.currentThread().getId();
        Socket socket = sockets.get(threadId);
        if (socket == null) {
            synchronized (sockets) {
                socket = sockets.get(threadId);
                if (socket == null) {
                    socket = new ReconnectingSocket();
                    sockets.put(threadId, socket);
                }
            }
        }
        return socket;
    }

    @Override
    public void close() {
        for (Socket socket : sockets.values()) {
            socket.close();
        }
    }

    public interface Socket {
        void send(byte[] msg);
        void send(String msg);
        byte[] recv();
        int recv(byte[] buffer, int offset, int len, int flags);
        String recvStr();
        boolean hasReceiveMore();
        void close();
    }

    public class ReconnectingSocket implements Socket {

        private int RECV_TIMEOUT = 1000;
        private ZMQ.Socket socket;
        private byte[] lastMessage;

        public ReconnectingSocket() {
            socket = context.createSocket(socketType);
            socket.connect(url);
            lastMessage = null;
            if (socket.getSocketType().equals(SocketType.REQ)) {
                socket.setReceiveTimeOut(RECV_TIMEOUT);
            }
        }

        @Override
        public void send(byte[] msg) {
            if (socket.getSocketType().equals(SocketType.REQ)) {
                lastMessage = msg;
            }
            socket.send(msg);
        }

        @Override
        public void send(String msg) {
            send(msg.getBytes());
        }

        @Override
        public byte[] recv() {
            byte[] msg = null;
            if (socket.getSocketType().equals(SocketType.REQ)) {
                try {
                    msg = socket.recv();
                    if (msg == null) {
                        socket.close();
                        socket.connect(url);
                        socket.setReceiveTimeOut(RECV_TIMEOUT);
                        socket.send(lastMessage);
                    }
                } catch (Exception e) {
                    log.error(e.getMessage() + ". Continuing.");
                    try {
                        socket.close();
                        socket.connect(url);
                        socket.setReceiveTimeOut(RECV_TIMEOUT);
                        socket.send(lastMessage);
                    } catch (Exception e2) {
                        log.error("Reusing socket did not work, throwing it away and creating a new one.");
                        socket = context.createSocket(SocketType.REQ);
                        socket.connect(url);
                        socket.setReceiveTimeOut(RECV_TIMEOUT);
                        socket.send(lastMessage);
                    }
                }
            } else {
                msg = socket.recv();
            }
            if (msg == null) {
                log.error("Still getting no response anymore, giving up.");
            }
            return msg;
        }

        @Override
        public int recv(byte[] buffer, int offset, int len, int flags) {
            return socket.recv(buffer, offset, len, flags);
        }

        @Override
        public String recvStr() {
            return new String(recv());
        }

        @Override
        public boolean hasReceiveMore() {
            return socket.hasReceiveMore();
        }

        @Override
        public void close() {
            socket.close();
        }
    }
}
