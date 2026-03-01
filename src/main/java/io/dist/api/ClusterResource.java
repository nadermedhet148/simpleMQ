package io.dist.api;

import io.dist.cluster.RaftService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

import java.util.List;
import java.util.stream.Collectors;

@Path("/api/cluster")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ClusterResource {

    @Inject
    RaftService raftService;

    @GET
    @Path("/leader")
    public Response getLeader() {
        RaftPeerId leaderId = raftService.getLeaderId();
        if (leaderId == null) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Leader not yet elected").build();
        }
        return Response.ok(leaderId.toString()).build();
    }

    @GET
    @Path("/peers")
    public List<String> getPeers() {
        return raftService.getPeers().stream()
                .map(p -> p.getId() + "=" + p.getAddress())
                .collect(Collectors.toList());
    }

    @POST
    @Path("/join")
    public Uni<Response> joinCluster(@QueryParam("id") String id, @QueryParam("address") String address) {
        return raftService.addPeer(id, address)
                .map(success -> {
                    if (success) {
                        return Response.ok("Node " + id + " joined cluster").build();
                    } else {
                        return Response.status(Response.Status.BAD_REQUEST).entity("Failed to join cluster. Ensure you are calling the LEADER.").build();
                    }
                });
    }

    @POST
    @Path("/leave")
    public Uni<Response> leaveCluster(@QueryParam("id") String id) {
        return raftService.removePeer(id)
                .map(success -> {
                    if (success) {
                        return Response.ok("Node " + id + " left cluster").build();
                    } else {
                        return Response.status(Response.Status.BAD_REQUEST).entity("Failed to leave cluster. Ensure you are calling the LEADER.").build();
                    }
                });
    }
}
