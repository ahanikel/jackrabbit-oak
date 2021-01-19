package org.apache.jackrabbit.oak.store.zeromq.dropwizard;

import com.codahale.metrics.annotation.Timed;
import org.apache.jackrabbit.oak.store.zeromq.NodeStateAggregator;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/nodestate")
@Produces(MediaType.APPLICATION_JSON)
public class NodeStateResource {
    private final NodeStateAggregator nodeStateAggregator;

    public NodeStateResource(NodeStateAggregator nodeStateAggregator) {
        this.nodeStateAggregator = nodeStateAggregator;
    }

    @GET
    @Timed
    public KeyValueRepresentation getNodeState(@QueryParam("uuid") String uuid) {
        String ret = nodeStateAggregator.getNodeStore().readNodeState(uuid).getSerialised();
        return new KeyValueRepresentation(uuid, ret);
    }
}
