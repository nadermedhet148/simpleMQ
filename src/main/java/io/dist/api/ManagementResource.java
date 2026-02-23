package io.dist.api;

import io.dist.model.Binding;
import io.dist.model.Exchange;
import io.dist.model.Queue;
import io.dist.service.QueueService;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;

@Path("/api/management")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ManagementResource {

    @Inject
    QueueService queueService;

    @GET
    @Path("/exchanges")
    public List<Exchange> listExchanges() {
        return queueService.listExchanges();
    }

    @POST
    @Path("/exchanges")
    public Response createExchange(Exchange exchange) {
        queueService.createExchange(exchange.name, exchange.type, exchange.durable);
        return Response.status(Response.Status.CREATED).build();
    }

    @DELETE
    @Path("/exchanges/{name}")
    public Response deleteExchange(@PathParam("name") String name) {
        queueService.deleteExchange(name);
        return Response.noContent().build();
    }

    @GET
    @Path("/queues")
    public List<Queue> listQueues() {
        return queueService.listQueues();
    }

    @POST
    @Path("/queues")
    public Response createQueue(Queue queue) {
        queueService.createQueue(queue.name, queue.queueGroup, queue.durable, queue.autoDelete);
        return Response.status(Response.Status.CREATED).build();
    }

    @DELETE
    @Path("/queues/{name}")
    public Response deleteQueue(@PathParam("name") String name) {
        queueService.deleteQueue(name);
        return Response.noContent().build();
    }

    @POST
    @Path("/bindings")
    public Response bind(Binding binding) {
        queueService.bind(binding.exchangeName, binding.queueName, binding.routingKey);
        return Response.status(Response.Status.CREATED).build();
    }

    @DELETE
    @Path("/bindings")
    public Response unbind(Binding binding) {
        queueService.unbind(binding.exchangeName, binding.queueName, binding.routingKey);
        return Response.noContent().build();
    }

    @GET
    @Path("/bindings")
    public List<Binding> listBindings() {
        return queueService.listBindings();
    }
}
