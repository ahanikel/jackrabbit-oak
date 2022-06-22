package org.apache.jackrabbit.oak.simple;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.jackrabbit.oak.run.cli.CommonOptions;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.File;
import java.util.List;

public class CommHubCommand implements Command {
    public static final String NAME = "comm-hub";

    private final String summary = "Starts the communication hub\n" +
            "Example:\n" +
            "java -jar oak-run.jar comm-hub tcp://localhost:8000 tcp://localhost:8001";

    @Override
    public void execute(String... args) throws Exception {

        try (
                ZContext ctx = new ZContext();
                ZMQ.Socket publisher = ctx.createSocket(SocketType.PUB);
                ZMQ.Socket subscriber = ctx.createSocket(SocketType.SUB)
        ) {
            final OptionParser parser = new OptionParser();

            final Options opts = new Options();
            opts.setCommandName(NAME);
            opts.setSummary(summary);

            final OptionSet optionSet = opts.parseAndConfigure(parser, args);
            final List<?> uris = optionSet.nonOptionArguments();

            if (uris.size() != 2) {
                throw new IllegalArgumentException(summary);
            }

            final CommonOptions commonOptions = opts.getOptionBean(CommonOptions.class);
            final String publisherUri = commonOptions.getURI(0).toString();
            final String subscriberUri = commonOptions.getURI(1).toString();
            publisher.bind(subscriberUri);
            subscriber.subscribe("".getBytes());
            subscriber.bind(publisherUri);
            ZMQ.proxy(subscriber, publisher, null);
        }
    }
}
