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

/**
 * REST endpoint for cluster management operations.
 *
 * <p>Provides read-only queries for the current leader and peer list, as well
 * as write operations to dynamically add or remove nodes from the Raft cluster.
 * Membership changes (join/leave) must be sent to the current leader node.</p>
 *
 * <h3>Endpoints</h3>
 * <ul>
 *   <li>{@code GET  /api/cluster/leader} – returns the current Raft leader ID</li>
 *   <li>{@code GET  /api/cluster/peers}  – lists all cluster peers</li>
 *   <li>{@code POST /api/cluster/join?id=...&address=...} – adds a new node</li>
 *   <li>{@code POST /api/cluster/leave?id=...} – removes a node</li>
 * </ul>
 *
 * @see RaftService
 */
@Path("/api/cluster")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ClusterResource {

    @Inject
    RaftService raftService;

    /**
     * Returns the ID of the current Raft leader.
     *
     * @return HTTP 200 with the leader ID, or 503 if no leader is elected yet
     */
    @GET
    @Path("/leader")
    public Response getLeader() {
        RaftPeerId leaderId = raftService.getLeaderId();
        if (leaderId == null) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Leader not yet elected").build();
        }
        return Response.ok(leaderId.toString()).build();
    }

    /**
     * Lists all known cluster peers in {@code "id=address"} format.
     *
     * @return list of peer strings
     */
    @GET
    @Path("/peers")
    public List<String> getPeers() {
        return raftService.getPeers().stream()
                .map(p -> p.getId() + "=" + p.getAddress())
                .collect(Collectors.toList());
    }

    /**
     * Adds a new node to the Raft cluster. Must be called on the leader.
     *
     * @param id      the new node's identifier
     * @param address the new node's Raft address (host:port)
     * @return HTTP 200 on success, or 400 if the operation fails
     */
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

    /**
     * Removes a node from the Raft cluster. Must be called on the leader.
     *
     * @param id the node identifier to remove
     * @return HTTP 200 on success, or 400 if the operation fails
     */
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
