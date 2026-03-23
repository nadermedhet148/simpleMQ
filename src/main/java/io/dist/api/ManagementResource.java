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

/**
 * REST endpoint for managing exchanges, queues, and bindings, as well as
 * retrieving a full dashboard summary of the broker's state.
 *
 * <p>All mutating operations (POST / DELETE) are forwarded through the Raft
 * consensus layer via {@link QueueService}. Read operations query the local
 * database and in-memory state.</p>
 *
 * <h3>Endpoints</h3>
 * <ul>
 *   <li>{@code GET  /api/management/summary}   – full broker dashboard summary</li>
 *   <li>{@code GET  /api/management/exchanges}  – list all exchanges</li>
 *   <li>{@code POST /api/management/exchanges}  – create an exchange</li>
 *   <li>{@code DELETE /api/management/exchanges/{name}} – delete an exchange</li>
 *   <li>{@code GET  /api/management/queues}     – list all queues</li>
 *   <li>{@code POST /api/management/queues}     – create a queue</li>
 *   <li>{@code DELETE /api/management/queues/{name}} – delete a queue</li>
 *   <li>{@code GET  /api/management/bindings}   – list all bindings</li>
 *   <li>{@code POST /api/management/bindings}   – create a binding</li>
 *   <li>{@code DELETE /api/management/bindings}  – remove a binding</li>
 * </ul>
 *
 * @see QueueService
 * @see ManagementSummary
 */
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

    /**
     * Returns a comprehensive summary of the broker's current state including
     * cluster info, all exchanges, all queues with message counts, and metrics.
     *
     * @return the management summary DTO
     */
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
                            .map(q -> {
                                int count = (q.queueType == io.dist.model.QueueType.STREAM)
                                        ? (int) storageService.getStreamBuffer(q.name).size()
                                        : storageService.getBuffer(q.name).size();
                                return new ManagementSummary.QueueInfo(
                                        q.name,
                                        q.queueGroup,
                                        count,
                                        q.durable,
                                        q.queueType != null ? q.queueType.name() : "STANDARD");
                            })
                            .collect(Collectors.toList());
                    summary.metrics = metricsService.getMetrics();
                    return summary;
                });
    }

    // ======================== Exchange CRUD ========================

    /**
     * Lists all registered exchanges.
     *
     * @return list of exchanges
     */
    @GET
    @Path("/exchanges")
    public Uni<List<Exchange>> listExchanges() {
        return queueService.listExchanges();
    }

    /**
     * Creates a new exchange. Replicated through Raft.
     *
     * @param exchange the exchange to create (name, type, durable)
     * @return HTTP 201 Created on success
     */
    @POST
    @Path("/exchanges")
    public Uni<Response> createExchange(Exchange exchange) {
        return queueService.createExchange(exchange.name, exchange.type, exchange.durable)
                .replaceWith(Response.status(Response.Status.CREATED).build());
    }

    /**
     * Deletes an exchange and its associated bindings. Replicated through Raft.
     *
     * @param name the exchange name to delete
     * @return HTTP 204 No Content on success
     */
    @DELETE
    @Path("/exchanges/{name}")
    public Uni<Response> deleteExchange(@PathParam("name") String name) {
        return queueService.deleteExchange(name)
                .replaceWith(Response.noContent().build());
    }

    // ======================== Queue CRUD ========================

    /**
     * Lists all registered queues.
     *
     * @return list of queues
     */
    @GET
    @Path("/queues")
    public Uni<List<Queue>> listQueues() {
        return queueService.listQueues();
    }

    /**
     * Creates a new queue. Replicated through Raft.
     *
     * @param queue the queue to create (name, queueGroup, durable, autoDelete)
     * @return HTTP 201 Created on success
     */
    @POST
    @Path("/queues")
    public Uni<Response> createQueue(Queue queue) {
        return queueService.createQueue(queue.name, queue.queueGroup, queue.durable, queue.autoDelete, queue.queueType)
                .replaceWith(Response.status(Response.Status.CREATED).build());
    }

    /**
     * Deletes a queue and its associated bindings. Replicated through Raft.
     *
     * @param name the queue name to delete
     * @return HTTP 204 No Content on success
     */
    @DELETE
    @Path("/queues/{name}")
    public Uni<Response> deleteQueue(@PathParam("name") String name) {
        return queueService.deleteQueue(name)
                .replaceWith(Response.noContent().build());
    }

    // ======================== Binding CRUD ========================

    /**
     * Creates a binding between an exchange and a queue. Replicated through Raft.
     *
     * @param binding the binding to create (exchangeName, queueName, routingKey)
     * @return HTTP 201 Created on success
     */
    @POST
    @Path("/bindings")
    public Uni<Response> bind(Binding binding) {
        return queueService.bind(binding.exchangeName, binding.queueName, binding.routingKey)
                .replaceWith(Response.status(Response.Status.CREATED).build());
    }

    /**
     * Removes a binding between an exchange and a queue. Replicated through Raft.
     *
     * @param binding the binding to remove (exchangeName, queueName, routingKey)
     * @return HTTP 204 No Content on success
     */
    @DELETE
    @Path("/bindings")
    public Uni<Response> unbind(Binding binding) {
        return queueService.unbind(binding.exchangeName, binding.queueName, binding.routingKey)
                .replaceWith(Response.noContent().build());
    }

    /**
     * Lists all registered bindings.
     *
     * @return list of bindings
     */
    @GET
    @Path("/bindings")
    public Uni<List<Binding>> listBindings() {
        return queueService.listBindings();
    }
}
