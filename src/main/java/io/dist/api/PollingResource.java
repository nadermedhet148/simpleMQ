package io.dist.api;

import io.dist.model.Message;
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
    public Response poll(@PathParam("queue") String queue) {
        Message msg = messagingEngine.poll(queue);
        if (msg == null) {
            return Response.status(Response.Status.NO_CONTENT).build();
        }
        return Response.ok(msg).build();
    }

    @POST
    @Path("/ack/{messageId}")
    public Response acknowledge(@PathParam("messageId") String messageId) {
        messagingEngine.acknowledgeMessage(messageId);
        return Response.ok().build();
    }

    @POST
    @Path("/nack/{messageId}")
    public Response nack(@PathParam("messageId") String messageId, @QueryParam("requeue") @DefaultValue("true") boolean requeue) {
        messagingEngine.nackMessage(messageId, requeue);
        return Response.ok().build();
    }
}
