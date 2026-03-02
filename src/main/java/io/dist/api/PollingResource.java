package io.dist.api;

import io.dist.model.Message;
import io.smallrye.mutiny.Uni;
import io.dist.service.MessagingEngine;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * REST endpoint for consumer operations: polling messages and sending
 * acknowledgements (ACK / NACK).
 *
 * <h3>Consumer Workflow</h3>
 * <ol>
 *   <li><b>Poll</b> – {@code GET /api/poll/{queue}} retrieves the next
 *       available message from the queue. Returns 204 No Content if empty.</li>
 *   <li><b>ACK</b> – {@code POST /api/poll/ack/{messageId}} confirms
 *       successful processing; the message is marked as done.</li>
 *   <li><b>NACK</b> – {@code POST /api/poll/nack/{messageId}?requeue=true|false}
 *       rejects the message. If {@code requeue=true} the message goes back
 *       to the queue; otherwise it is routed to the Dead Letter Queue.</li>
 * </ol>
 *
 * @see MessagingEngine
 */
@Path("/api/poll")
@Produces(MediaType.APPLICATION_JSON)
public class PollingResource {

    @Inject
    MessagingEngine messagingEngine;

    /**
     * Polls the next available message from the specified queue.
     *
     * @param queue the queue name to poll from
     * @return HTTP 200 with the message JSON, or 204 No Content if the queue is empty
     */
    @GET
    @Path("/{queue}")
    public Uni<Response> poll(@PathParam("queue") String queue) {
        return messagingEngine.poll(queue)
                .map(msg -> {
                    if (msg == null) {
                        return Response.status(Response.Status.NO_CONTENT).build();
                    }
                    return Response.ok(msg).build();
                });
    }

    /**
     * Acknowledges a message, marking it as successfully processed.
     *
     * @param messageId the ID of the message to acknowledge
     * @return HTTP 200 OK on success
     */
    @POST
    @Path("/ack/{messageId}")
    public Uni<Response> acknowledge(@PathParam("messageId") String messageId) {
        return messagingEngine.acknowledgeMessage(messageId)
                .replaceWith(Response.ok().build());
    }

    /**
     * Negatively acknowledges a message.
     *
     * @param messageId the ID of the message to nack
     * @param requeue   whether to requeue the message ({@code true}, default)
     *                  or route it to the Dead Letter Queue ({@code false})
     * @return HTTP 200 OK on success
     */
    @POST
    @Path("/nack/{messageId}")
    public Uni<Response> nack(@PathParam("messageId") String messageId, @QueryParam("requeue") @DefaultValue("true") boolean requeue) {
        return messagingEngine.nackMessage(messageId, requeue)
                .replaceWith(Response.ok().build());
    }
}
