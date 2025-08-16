package org.apache.jackrabbit.oak.store.zeromq.cli;

import org.apache.jackrabbit.oak.store.zeromq.Constants;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import picocli.CommandLine;

@CommandLine.Command(
        name = "comm-hub",
        mixinStandardHelpOptions = true,
description = "Run the backend comunication hub")
public class CommHubCommand implements Runnable {

  @CommandLine.Option(names = {"-s", "--sending-uri"}, description = "URI listening for messages (default: ${DEFAULT-VALUE})")
  String sendingUri = Constants.DEFAULT_BACKEND_WRITER_URL;

  @CommandLine.Option(names = {"-r", "--receiving-uri"}, description = "URI sending messages to (default: ${DEFAULT-VALUE})")
  String receivingUri = Constants.DEFAULT_BACKEND_READER_URL;

  @Override
  public void run() {
    try (
            ZContext ctx = new ZContext();
            ZMQ.Socket publisher = ctx.createSocket(SocketType.PUB);
            ZMQ.Socket subscriber = ctx.createSocket(SocketType.SUB)
    ) {
      publisher.bind(receivingUri);
      subscriber.subscribe("".getBytes());
      subscriber.bind(sendingUri);
      ZMQ.proxy(subscriber, publisher, null);
    }
  }
}
