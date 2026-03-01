package io.dist.api;

import io.dist.model.Message;
import io.smallrye.mutiny.Uni;
import io.dist.service.MessagingEngine;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/api/poll")
@Produces(MediaType.APPLICATION_JSON)
public class PollingResource {

    @Inject
    MessagingEngine messagingEngine;

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

    @POST
    @Path("/ack/{messageId}")
    public Uni<Response> acknowledge(@PathParam("messageId") String messageId) {
        return messagingEngine.acknowledgeMessage(messageId)
                .replaceWith(Response.ok().build());
    }

    @POST
    @Path("/nack/{messageId}")
    public Uni<Response> nack(@PathParam("messageId") String messageId, @QueryParam("requeue") @DefaultValue("true") boolean requeue) {
        return messagingEngine.nackMessage(messageId, requeue)
                .replaceWith(Response.ok().build());
    }
}
