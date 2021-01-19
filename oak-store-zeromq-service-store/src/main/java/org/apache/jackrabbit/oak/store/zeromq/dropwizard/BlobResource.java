package org.apache.jackrabbit.oak.store.zeromq.dropwizard;

import com.codahale.metrics.annotation.Timed;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.store.zeromq.NodeStateAggregator;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/blob")
@Produces(MediaType.APPLICATION_OCTET_STREAM)
public class BlobResource {
    private final NodeStateAggregator nodeStateAggregator;

    public BlobResource(NodeStateAggregator nodeStateAggregator) {
        this.nodeStateAggregator = nodeStateAggregator;
    }

    @GET
    @Timed
    public StreamRepresentation getBlob(@QueryParam("reference") String reference) {
        final Blob ret = nodeStateAggregator.getNodeStore().getBlob(reference);
        return new StreamRepresentation(ret);
    }
}
