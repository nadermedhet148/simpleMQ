package io.dist.api;

import io.dist.cluster.RaftService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Response;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.server.ServerRequestFilter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

@ApplicationScoped
public class LeaderProxyFilter {
    private static final Logger LOG = Logger.getLogger(LeaderProxyFilter.class);

    @Inject
    RaftService raftService;

    private static final Client client = ClientBuilder.newClient();

    @ServerRequestFilter(preMatching = true)
    public Uni<Response> filter(ContainerRequestContext requestContext) {
        String rawPath = requestContext.getUriInfo().getPath();
        String path = rawPath.startsWith("/") ? rawPath.substring(1) : rawPath;
        String method = requestContext.getMethod();

        LOG.debugf("Checking proxy for %s %s (normalized: %s)", method, rawPath, path);

        // Only proxy write operations to the leader
        if (!isWriteOperation(method, path)) {
            return Uni.createFrom().nullItem();
        }

        if (raftService.isLeader()) {
            LOG.debugf("Node is leader, not proxying %s", path);
            return Uni.createFrom().nullItem();
        }

        RaftPeerId leaderId = raftService.getLeaderId();
        if (leaderId == null) {
            return Uni.createFrom().item(Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity("Leader not elected yet").build());
        }

        RaftPeer leader = raftService.getPeers().stream()
                .filter(p -> p.getId().equals(leaderId))
                .findFirst()
                .orElse(null);

        if (leader == null) {
            LOG.warnf("Leader %s not found in peers list", leaderId);
            return Uni.createFrom().nullItem();
        }

        String raftAddress = leader.getAddress();
        String httpAddress = raftAddress.split(":")[0] + ":8080";
        String targetUrl = "http://" + httpAddress + "/" + path;
        
        String query = requestContext.getUriInfo().getRequestUri().getQuery();
        if (query != null && !query.isEmpty()) {
            targetUrl += "?" + query;
        }

        LOG.infof("Proxying %s %s to leader at %s (Thread: %s)", method, rawPath, targetUrl, Thread.currentThread().getName());

        return forwardRequest(requestContext, targetUrl);
    }

    private boolean isWriteOperation(String method, String path) {
        if (path.startsWith("api/management") || path.startsWith("api/publish") || path.startsWith("api/poll") || path.startsWith("api/cluster")) {
            return !method.equalsIgnoreCase("GET") || path.startsWith("api/poll");
        }
        return false;
    }

    private Uni<Response> forwardRequest(ContainerRequestContext context, String url) {
        return Uni.createFrom().item(() -> {
            var invocationBuilder = client.target(url).request();

            // Copy headers
            for (var entry : context.getHeaders().entrySet()) {
                String key = entry.getKey();
                if (!key.equalsIgnoreCase("Host") && !key.equalsIgnoreCase("Content-Length")) {
                    invocationBuilder.header(key, entry.getValue().get(0));
                }
            }

            String method = context.getMethod();
            try {
                if (context.hasEntity()) {
                    byte[] bytes = context.getEntityStream().readAllBytes();
                    // Re-populate original entity stream
                    context.setEntityStream(new ByteArrayInputStream(bytes));
                    return invocationBuilder.method(method, Entity.entity(bytes, context.getMediaType()));
                } else {
                    return invocationBuilder.method(method);
                }
            } catch (IOException e) {
                throw new RuntimeException("Error reading request entity", e);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .onFailure().transform(e -> {
            LOG.errorf(e, "Error proxying %s request to leader at %s", context.getMethod(), url);
            return new RuntimeException("Error proxying to leader: " + e.getMessage(), e);
        });
    }
}
