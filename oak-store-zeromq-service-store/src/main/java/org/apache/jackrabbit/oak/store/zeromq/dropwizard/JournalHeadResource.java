package org.apache.jackrabbit.oak.store.zeromq.dropwizard;

import com.codahale.metrics.annotation.Timed;
import org.apache.jackrabbit.oak.store.zeromq.NodeStateAggregator;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/journal-head")
@Produces(MediaType.APPLICATION_JSON)
public class JournalHeadResource {
    private final NodeStateAggregator nodeStateAggregator;

    public JournalHeadResource(NodeStateAggregator nodeStateAggregator) {
        this.nodeStateAggregator = nodeStateAggregator;
    }

    @GET
    @Timed
    public KeyValueRepresentation getJournalHead(@QueryParam("journal") String journal) {
        final String ret = nodeStateAggregator.getJournalHead(journal);
        return new KeyValueRepresentation(journal, ret);
    }
}
