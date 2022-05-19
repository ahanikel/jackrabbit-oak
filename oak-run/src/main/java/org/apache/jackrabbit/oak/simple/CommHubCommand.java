package org.apache.jackrabbit.oak.simple;

import org.apache.jackrabbit.oak.run.commons.Command;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class CommHubCommand implements Command {
    public static final String NAME = "comm-hub";

    private final String summary = "Starts the communication hub\n" +
            "Example:\n" +
            "java -jar oak-run.jar comm-hub";

    @Override
    public void execute(String... args) throws Exception {

        try (
                ZContext ctx = new ZContext();
                ZMQ.Socket publisher = ctx.createSocket(SocketType.PUB);
                ZMQ.Socket subscriber = ctx.createSocket(SocketType.SUB)
        ) {
            publisher.bind("tcp://*:8000");
            subscriber.subscribe("".getBytes());
            subscriber.bind("tcp://*:8001");
            ZMQ.proxy(publisher, subscriber, null);
        }
    }
}
