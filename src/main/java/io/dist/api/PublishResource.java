package io.dist.api;

import io.dist.service.MessagingEngine;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/api/publish")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class PublishResource {

    @Inject
    MessagingEngine messagingEngine;

    public static class PublishRequest {
        public String routingKey;
        public String payload;
    }

    @POST
    @Path("/{exchange}")
    public Uni<Response> publish(@PathParam("exchange") String exchange, PublishRequest request) {
        return messagingEngine.publish(exchange, request.routingKey, request.payload)
                .replaceWith(Response.accepted().build());
    }
}
