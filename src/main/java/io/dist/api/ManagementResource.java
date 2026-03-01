package io.dist.api;

import io.dist.model.*;
import io.dist.service.MetricsService;
import io.smallrye.mutiny.Uni;
import io.dist.service.QueueService;
import io.dist.cluster.RaftService;
import io.dist.storage.StorageService;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.stream.Collectors;

@Path("/api/management")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ManagementResource {

    @Inject
    QueueService queueService;

    @Inject
    RaftService raftService;

    @Inject
    StorageService storageService;

    @Inject
    MetricsService metricsService;

    @GET
    @Path("/summary")
    public Uni<ManagementSummary> getSummary() {
        ManagementSummary summary = new ManagementSummary();
        summary.nodeId = raftService.getNodeId();
        summary.isLeader = raftService.isLeader();
        summary.leaderId = raftService.getLeaderId() != null ? raftService.getLeaderId().toString() : "None";
        summary.peers = raftService.getPeers().stream()
                .map(p -> p.getId() + "=" + p.getAddress())
                .collect(Collectors.toList());

        // Chain DB calls sequentially to avoid concurrent SQLite access
        return queueService.listExchanges()
                .flatMap(exchanges -> {
                    summary.exchanges = exchanges;
                    return queueService.listQueues();
                })
                .map(queues -> {
                    summary.queues = queues.stream()
                            .map(q -> new ManagementSummary.QueueInfo(
                                    q.name,
                                    q.queueGroup,
                                    storageService.getBuffer(q.name).size(),
                                    q.durable))
                            .collect(Collectors.toList());
                    summary.metrics = metricsService.getMetrics();
                    return summary;
                });
    }

    @GET
    @Path("/exchanges")
    public Uni<List<Exchange>> listExchanges() {
        return queueService.listExchanges();
    }

    @POST
    @Path("/exchanges")
    public Uni<Response> createExchange(Exchange exchange) {
        return queueService.createExchange(exchange.name, exchange.type, exchange.durable)
                .replaceWith(Response.status(Response.Status.CREATED).build());
    }

    @DELETE
    @Path("/exchanges/{name}")
    public Uni<Response> deleteExchange(@PathParam("name") String name) {
        return queueService.deleteExchange(name)
                .replaceWith(Response.noContent().build());
    }

    @GET
    @Path("/queues")
    public Uni<List<Queue>> listQueues() {
        return queueService.listQueues();
    }

    @POST
    @Path("/queues")
    public Uni<Response> createQueue(Queue queue) {
        return queueService.createQueue(queue.name, queue.queueGroup, queue.durable, queue.autoDelete)
                .replaceWith(Response.status(Response.Status.CREATED).build());
    }

    @DELETE
    @Path("/queues/{name}")
    public Uni<Response> deleteQueue(@PathParam("name") String name) {
        return queueService.deleteQueue(name)
                .replaceWith(Response.noContent().build());
    }

    @POST
    @Path("/bindings")
    public Uni<Response> bind(Binding binding) {
        return queueService.bind(binding.exchangeName, binding.queueName, binding.routingKey)
                .replaceWith(Response.status(Response.Status.CREATED).build());
    }

    @DELETE
    @Path("/bindings")
    public Uni<Response> unbind(Binding binding) {
        return queueService.unbind(binding.exchangeName, binding.queueName, binding.routingKey)
                .replaceWith(Response.noContent().build());
    }

    @GET
    @Path("/bindings")
    public Uni<List<Binding>> listBindings() {
        return queueService.listBindings();
    }
}
