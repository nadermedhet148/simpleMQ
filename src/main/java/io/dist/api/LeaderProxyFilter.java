package io.dist.api;

import io.dist.cluster.RaftService;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.jboss.logging.Logger;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MultivaluedMap;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

@Provider
@PreMatching
public class LeaderProxyFilter implements ContainerRequestFilter {
    private static final Logger LOG = Logger.getLogger(LeaderProxyFilter.class);

    @Inject
    RaftService raftService;

    private static final Client client = ClientBuilder.newClient();

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        String rawPath = requestContext.getUriInfo().getPath();
        String path = rawPath.startsWith("/") ? rawPath.substring(1) : rawPath;
        String method = requestContext.getMethod();

        LOG.debugf("Checking proxy for %s %s (normalized: %s)", method, rawPath, path);

        // Only proxy write operations to the leader
        if (!isWriteOperation(method, path)) {
            return;
        }

        if (raftService.isLeader()) {
            LOG.debugf("Node is leader, not proxying %s", path);
            return;
        }

        RaftPeerId leaderId = raftService.getLeaderId();
        if (leaderId == null) {
            requestContext.abortWith(Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity("Leader not elected yet").build());
            return;
        }

        RaftPeer leader = raftService.getPeers().stream()
                .filter(p -> p.getId().equals(leaderId))
                .findFirst()
                .orElse(null);

        if (leader == null) {
            LOG.warnf("Leader %s not found in peers list", leaderId);
            return;
        }

        // Derive HTTP address from Raft address (assuming port mapping or convention)
        // Raft address is nodeX:9851, HTTP is nodeX:8080
        String raftAddress = leader.getAddress();
        String httpAddress = raftAddress.split(":")[0] + ":8080";
        String targetUrl = "http://" + httpAddress + "/" + path;
        
        // Add query parameters if any
        String query = requestContext.getUriInfo().getRequestUri().getQuery();
        if (query != null && !query.isEmpty()) {
            targetUrl += "?" + query;
        }

        LOG.infof("Proxying %s %s to leader at %s", method, rawPath, targetUrl);

        try {
            Response leaderResponse = forwardRequest(requestContext, targetUrl);
            requestContext.abortWith(leaderResponse);
        } catch (Exception e) {
            LOG.errorf(e, "Failed to proxy request to leader at %s", targetUrl);
            requestContext.abortWith(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Error proxying to leader: " + e.getMessage()).build());
        }
    }

    private boolean isWriteOperation(String method, String path) {
        // Management operations: POST/DELETE/PUT
        // Publish: POST
        // Poll: GET (but it's state-modifying in our case, so we must proxy it to leader!)
        // Ack/Nack: POST
        // Summary: GET (should be proxied to leader for consistent metrics)
        
        if (path.startsWith("api/management") || path.startsWith("api/publish") || path.startsWith("api/poll")) {
            if (path.equals("api/management/summary")) return true;
            return !method.equalsIgnoreCase("GET") || path.startsWith("api/poll");
        }
        return false;
    }

    private Response forwardRequest(ContainerRequestContext context, String url) throws IOException {
        var invocation = client.target(url)
                .request();

        // Copy headers
        MultivaluedMap<String, String> headers = context.getHeaders();
        for (var entry : headers.entrySet()) {
            if (!entry.getKey().equalsIgnoreCase("Host") && !entry.getKey().equalsIgnoreCase("Content-Length")) {
                invocation.header(entry.getKey(), entry.getValue().get(0));
            }
        }

        String method = context.getMethod();
        if (context.hasEntity()) {
            byte[] entityBytes = context.getEntityStream().readAllBytes();
            // Re-populate original entity stream because we read it
            context.setEntityStream(new ByteArrayInputStream(entityBytes));
            return invocation.method(method, Entity.entity(entityBytes, context.getMediaType()));
        } else {
            return invocation.method(method);
        }
    }
}
