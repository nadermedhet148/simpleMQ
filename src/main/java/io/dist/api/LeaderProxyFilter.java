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

/**
 * Pre-matching request filter that transparently proxies write operations
 * to the current Raft leader node.
 *
 * <p>In a multi-node simpleMQ cluster, only the leader can accept write
 * operations (publish, poll, management mutations). When a follower receives
 * such a request this filter intercepts it <em>before</em> the JAX-RS
 * resource method is invoked and forwards the entire HTTP request to the
 * leader's HTTP endpoint. The response from the leader is then returned
 * directly to the client.</p>
 *
 * <h3>Which requests are proxied?</h3>
 * <ul>
 *   <li>All non-GET requests to {@code /api/management/*}, {@code /api/publish/*},
 *       and {@code /api/cluster/*}</li>
 *   <li>All requests (including GET) to {@code /api/poll/*} because polling
 *       is a write operation (it dequeues a message)</li>
 * </ul>
 *
 * <p>GET requests to management and cluster endpoints are <b>not</b> proxied
 * and are served locally from the follower's own state.</p>
 *
 * @see RaftService#isLeader()
 * @see RaftService#getLeaderId()
 */
@ApplicationScoped
public class LeaderProxyFilter {
    private static final Logger LOG = Logger.getLogger(LeaderProxyFilter.class);

    @Inject
    RaftService raftService;

    /** Shared JAX-RS client used for proxying requests to the leader. */
    private static final Client client = ClientBuilder.newClient();

    /**
     * Pre-matching filter that checks every incoming request and proxies
     * write operations to the leader if this node is a follower.
     *
     * @param requestContext the incoming request context
     * @return a {@link Response} from the leader if proxied, or {@code null}
     *         to continue normal processing on this node
     */
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

        // If this node is the leader, no proxying needed
        if (raftService.isLeader()) {
            LOG.debugf("Node is leader, not proxying %s", path);
            return Uni.createFrom().nullItem();
        }

        // Determine the leader's address
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

        // Build the target URL using the leader's HTTP port (assumed to be 8080)
        String raftAddress = leader.getAddress();
        String httpAddress = raftAddress.split(":")[0] + ":8080";
        String targetUrl = "http://" + httpAddress + "/" + path;
        
        // Preserve query parameters
        String query = requestContext.getUriInfo().getRequestUri().getQuery();
        if (query != null && !query.isEmpty()) {
            targetUrl += "?" + query;
        }

        LOG.infof("Proxying %s %s to leader at %s (Thread: %s)", method, rawPath, targetUrl, Thread.currentThread().getName());

        return forwardRequest(requestContext, targetUrl);
    }

    /**
     * Determines whether a request is a write operation that must be handled
     * by the leader.
     *
     * @param method the HTTP method (GET, POST, DELETE, etc.)
     * @param path   the normalized request path (without leading slash)
     * @return {@code true} if the request should be proxied to the leader
     */
    private boolean isWriteOperation(String method, String path) {
        if (path.startsWith("api/management") || path.startsWith("api/publish")
                || path.startsWith("api/poll") || path.startsWith("api/cluster")
                || path.startsWith("api/processors")) {
            // All poll requests are writes (dequeue); for other paths only non-GET are writes
            return !method.equalsIgnoreCase("GET") || path.startsWith("api/poll");
        }
        return false;
    }

    /**
     * Forwards the incoming request to the leader node and returns the
     * leader's response.
     *
     * <p>Headers are copied (except Host and Content-Length which are
     * recalculated). The request entity body is read, forwarded, and then
     * restored on the original stream in case downstream processing needs it.</p>
     *
     * @param context the original request context
     * @param url     the leader's target URL
     * @return a Uni emitting the leader's response
     */
    private Uni<Response> forwardRequest(ContainerRequestContext context, String url) {
        return Uni.createFrom().item(() -> {
            var invocationBuilder = client.target(url).request();

            // Copy headers from the original request (skip Host and Content-Length)
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
                    // Re-populate original entity stream for potential downstream use
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
