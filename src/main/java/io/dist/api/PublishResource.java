package io.dist.api;

import io.dist.service.MessagingEngine;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * REST endpoint for publishing messages to an exchange.
 *
 * <p>Producers send a POST request with a JSON body containing the
 * {@code routingKey} and {@code payload}. The message is routed to the
 * appropriate queues based on the exchange type and binding configuration.</p>
 *
 * <h3>Example</h3>
 * <pre>
 * POST /api/publish/my-exchange
 * {
 *   "routingKey": "order.created",
 *   "payload": "{\"orderId\": 123}"
 * }
 * </pre>
 *
 * @see MessagingEngine#publish(String, String, String)
 */
@Path("/api/publish")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class PublishResource {

    @Inject
    MessagingEngine messagingEngine;

    /**
     * Request body for the publish endpoint.
     */
    public static class PublishRequest {
        /** Routing key used for DIRECT exchange matching. */
        public String routingKey;
        /** The message body / content to publish. */
        public String payload;
    }

    /**
     * Publishes a message to the specified exchange.
     *
     * @param exchange the target exchange name (path parameter)
     * @param request  the publish request containing routing key and payload
     * @return HTTP 202 Accepted on success
     */
    @POST
    @Path("/{exchange}")
    public Uni<Response> publish(@PathParam("exchange") String exchange, PublishRequest request) {
        return messagingEngine.publish(exchange, request.routingKey, request.payload)
                .replaceWith(Response.accepted().build());
    }
}
