package org.apache.jackrabbit.oak.store.zeromq;

import org.junit.Ignore;
import org.junit.Test;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.io.InputStream;

public class ManualTests {

    @Test
    @Ignore
    public void testRaw1() throws InterruptedException {
        ZContext ctx = new ZContext();
        Thread t = new Thread() {
            public void run() {
                ZMQ.Socket s = ctx.createSocket(SocketType.SUB);
                s.connect("tcp://localhost:9000");
                s.subscribe("");
                for (int i = 0; i < 100; ++i) {
                    System.out.println("Receiving...");
                    System.out.println(s.recvStr());
                }
            }
        };
        t.setDaemon(true);
        t.start();
        ZMQ.Socket sock = ctx.createSocket(SocketType.PUB);
        System.out.println(sock.bind("tcp://*:9000"));
        Thread.sleep(1000);
        System.out.println(sock.sendMore("topic"));
        System.out.println(sock.send("hello"));
        Thread.sleep(1000);
    }

    @Test
    @Ignore
    public void testRaw2() throws InterruptedException {
        ZContext ctx = new ZContext();
        Thread t = new Thread() {
            public void run() {
                ZMQ.Socket s = ctx.createSocket(SocketType.SUB);
                s.bind("tcp://localhost:9000");
                s.subscribe("");
                for (int i = 0; i < 100; ++i) {
                    System.out.println("Receiving...");
                    System.out.println(s.recvStr());
                }
            }
        };
        t.setDaemon(true);
        t.start();
        ZMQ.Socket sock = ctx.createSocket(SocketType.PUB);
        System.out.println(sock.connect("tcp://localhost:9000"));
        Thread.sleep(1000);
        System.out.println(sock.sendMore("topic"));
        System.out.println(sock.send("hello"));
        Thread.sleep(1000);
    }

    @Test
    @Ignore
    public void testRaw3() throws InterruptedException {
        ZContext ctx = new ZContext();
        Thread t = new Thread() {
            public void run() {
                ZMQ.Socket s = ctx.createSocket(SocketType.SUB);
                s.bind("tcp://localhost:8000");
                s.subscribe("");
                for (int i = 0; i < 100; ++i) {
                    System.out.println("Receiving...");
                    System.out.println(s.recvStr());
                }
            }
        };
        t.setDaemon(true);
        t.start();
        ZMQ.Socket sock = ctx.createSocket(SocketType.PUB);
        System.out.println(sock.connect("tcp://localhost:8001"));
        Thread.sleep(1000);
        System.out.println(sock.sendMore("topic"));
        System.out.println(sock.send("hello"));
        Thread.sleep(1000);
    }

    @Test
    @Ignore
    public void testConnections() {
        SimpleRequestResponse r = new SimpleRequestResponse(SimpleRequestResponse.Topic.READ, "tcp://comm-hub:8001", "tcp://comm-hub:8000");
        r.requestString("journal", "golden");
        System.out.println(r.receiveMore());
    }

    @Test
    @Ignore
    public void testBlob() throws IOException {
        SimpleRequestResponse r = new SimpleRequestResponse(SimpleRequestResponse.Topic.READ, "tcp://comm-hub:8001", "tcp://comm-hub:8000");
        InputStream is = new ZeroMQBlobInputStream(r, "63C27F6741E5B9552BBCD2E6FAD68083");
        LoggingHook.writeBlob("63C27F6741E5B9552BBCD2E6FAD68083", is, (String op, byte[] args) -> System.out.println(op + " " + new String(args)));
    }
}
