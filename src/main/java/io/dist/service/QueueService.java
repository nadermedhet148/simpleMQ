package io.dist.service;

import io.dist.model.Binding;
import io.dist.model.Exchange;
import io.dist.model.ExchangeType;
import io.dist.model.Queue;
import io.dist.model.QueueType;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.util.List;

/**
 * Service layer for managing exchanges, queues, and bindings.
 *
 * <p>All mutating operations (create, delete, bind, unbind) are replicated
 * through the Raft consensus layer to ensure consistency across the cluster.
 * Each public method first checks that this node is the Raft leader; if not,
 * the request is rejected and the client should retry against the leader.</p>
 *
 * <p>The corresponding {@code *Local} methods perform the actual database
 * writes and are called by the {@link io.dist.cluster.SimpleStateMachine}
 * when a replicated log entry is applied on every node in the cluster.</p>
 *
 * @see io.dist.cluster.RaftService
 * @see io.dist.api.ManagementResource
 */
@ApplicationScoped
public class QueueService {

    @Inject
    io.dist.cluster.RaftService raftService;

    // ======================== Exchange Operations ========================

    /**
     * Creates a new exchange by replicating the command through Raft.
     * Only the leader can accept this request.
     *
     * @param name    unique exchange name
     * @param type    exchange type (DIRECT or FANOUT)
     * @param durable whether the exchange survives broker restarts
     * @return a Uni that completes when replication succeeds
     */
    public Uni<Void> createExchange(String name, ExchangeType type, boolean durable) {
        return raftService.replicateCreateExchange(name, type.name(), durable)
                .flatMap(success -> {
                    if (!success) return Uni.createFrom().failure(new RuntimeException("Failed to replicate exchange creation"));
                    return Uni.createFrom().voidItem();
                });
    }

    /**
     * Local (non-replicated) exchange creation applied by the state machine.
     * Idempotent: silently skips if the exchange already exists.
     *
     * @param name    exchange name
     * @param type    exchange type
     * @param durable durability flag
     */
    @Transactional
    public void createExchangeLocal(String name, ExchangeType type, boolean durable) {
        if (Exchange.findById(name) == null) {
            new Exchange(name, type, durable).persist();
        }
    }

    /**
     * Deletes an exchange by replicating the command through Raft.
     *
     * @param name the exchange name to delete
     * @return a Uni that completes when replication succeeds
     */
    public Uni<Void> deleteExchange(String name) {
        return raftService.replicateDeleteExchange(name)
                .flatMap(success -> {
                    if (!success) return Uni.createFrom().failure(new RuntimeException("Failed to replicate exchange deletion"));
                    return Uni.createFrom().voidItem();
                });
    }

    /**
     * Local exchange deletion applied by the state machine.
     * Also removes all bindings associated with the exchange.
     *
     * @param name the exchange name to delete
     */
    @Transactional
    public void deleteExchangeLocal(String name) {
        Exchange.deleteById(name);
        Binding.delete("exchangeName", name);
    }

    // ======================== Queue Operations ========================

    /**
     * Creates a new STANDARD queue by replicating the command through Raft.
     * Convenience overload that defaults to {@link QueueType#STANDARD}.
     *
     * @param name       unique queue name
     * @param group      logical group for the queue
     * @param durable    whether the queue survives broker restarts
     * @param autoDelete whether the queue auto-deletes when unused
     * @return a Uni that completes when replication succeeds
     */
    public Uni<Void> createQueue(String name, String group, boolean durable, boolean autoDelete) {
        return createQueue(name, group, durable, autoDelete, QueueType.STANDARD);
    }

    /**
     * Creates a new queue by replicating the command through Raft.
     *
     * @param name       unique queue name
     * @param group      logical group for the queue
     * @param durable    whether the queue survives broker restarts
     * @param autoDelete whether the queue auto-deletes when unused
     * @param queueType  the queue type (STANDARD or STREAM); defaults to STANDARD if null
     * @return a Uni that completes when replication succeeds
     */
    public Uni<Void> createQueue(String name, String group, boolean durable, boolean autoDelete, QueueType queueType) {
        QueueType type = queueType != null ? queueType : QueueType.STANDARD;
        return raftService.replicateCreateQueue(name, group, durable, autoDelete, type)
                .flatMap(success -> {
                    if (!success) return Uni.createFrom().failure(new RuntimeException("Failed to replicate queue creation"));
                    return Uni.createFrom().voidItem();
                });
    }

    /**
     * Local queue creation applied by the state machine.
     * Idempotent: silently skips if the queue already exists.
     *
     * @param name       queue name
     * @param group      logical group
     * @param durable    durability flag
     * @param autoDelete auto-delete flag
     * @param queueType  queue type (STANDARD or STREAM)
     */
    @Transactional
    public void createQueueLocal(String name, String group, boolean durable, boolean autoDelete, QueueType queueType) {
        if (Queue.findById(name) == null) {
            QueueType type = queueType != null ? queueType : QueueType.STANDARD;
            new Queue(name, group, durable, autoDelete, type).persist();
        }
    }

    /**
     * Deletes a queue by replicating the command through Raft.
     *
     * @param name the queue name to delete
     * @return a Uni that completes when replication succeeds
     */
    public Uni<Void> deleteQueue(String name) {
        return raftService.replicateDeleteQueue(name)
                .flatMap(success -> {
                    if (!success) return Uni.createFrom().failure(new RuntimeException("Failed to replicate queue deletion"));
                    return Uni.createFrom().voidItem();
                });
    }

    /**
     * Local queue deletion applied by the state machine.
     * Also removes all bindings associated with the queue.
     *
     * @param name the queue name to delete
     */
    @Transactional
    public void deleteQueueLocal(String name) {
        Queue.deleteById(name);
        Binding.delete("queueName", name);
    }

    // ======================== Binding Operations ========================

    /**
     * Creates a binding between an exchange and a queue by replicating through Raft.
     *
     * @param exchangeName the exchange to bind from
     * @param queueName    the queue to bind to
     * @param routingKey   the routing key for DIRECT exchanges (may be {@code null} for FANOUT)
     * @return a Uni that completes when replication succeeds
     */
    public Uni<Void> bind(String exchangeName, String queueName, String routingKey) {
        return raftService.replicateBind(exchangeName, queueName, routingKey)
                .flatMap(success -> {
                    if (!success) return Uni.createFrom().failure(new RuntimeException("Failed to replicate binding"));
                    return Uni.createFrom().voidItem();
                });
    }

    /**
     * Local binding creation applied by the state machine.
     * Idempotent: skips if an identical binding already exists.
     *
     * @param exchangeName exchange name
     * @param queueName    queue name
     * @param routingKey   routing key (may be {@code null})
     */
    @Transactional
    public void bindLocal(String exchangeName, String queueName, String routingKey) {
        if (Binding.find("exchangeName = ?1 and queueName = ?2 and (routingKey = ?3 or routingKey is null and ?3 is null or ?3 = '')", exchangeName, queueName, routingKey).count() == 0) {
            Binding binding = new Binding(exchangeName, queueName, (routingKey == null || routingKey.isEmpty()) ? null : routingKey);
            binding.persist();
        }
    }

    /**
     * Removes a binding between an exchange and a queue by replicating through Raft.
     *
     * @param exchangeName the exchange name
     * @param queueName    the queue name
     * @param routingKey   the routing key to match
     * @return a Uni that completes when replication succeeds
     */
    public Uni<Void> unbind(String exchangeName, String queueName, String routingKey) {
        return raftService.replicateUnbind(exchangeName, queueName, routingKey)
                .flatMap(success -> {
                    if (!success) return Uni.createFrom().failure(new RuntimeException("Failed to replicate unbinding"));
                    return Uni.createFrom().voidItem();
                });
    }

    /**
     * Local binding removal applied by the state machine.
     *
     * @param exchangeName exchange name
     * @param queueName    queue name
     * @param routingKey   routing key to match
     */
    @Transactional
    public void unbindLocal(String exchangeName, String queueName, String routingKey) {
        Binding.delete("exchangeName = ?1 and queueName = ?2 and (routingKey = ?3 or (routingKey is null and (?3 is null or ?3 = '')))", exchangeName, queueName, routingKey);
    }

    // ======================== Query Operations ========================

    /**
     * Lists all registered queues.
     *
     * @return a Uni emitting the list of all queues
     */
    public Uni<List<Queue>> listQueues() {
        return Uni.createFrom().item(() -> (List<Queue>)(List<?>)Queue.listAll())
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }
    
    /**
     * Lists all queues belonging to a specific group.
     *
     * @param group the group name to filter by
     * @return a Uni emitting the filtered list of queues
     */
    public Uni<List<Queue>> listQueuesInGroup(String group) {
        return Uni.createFrom().item(() -> (List<Queue>)(List<?>)Queue.find("queueGroup", group).list())
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Lists all registered exchanges.
     *
     * @return a Uni emitting the list of all exchanges
     */
    public Uni<List<Exchange>> listExchanges() {
        return Uni.createFrom().item(() -> (List<Exchange>)(List<?>)Exchange.listAll())
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Lists all registered bindings.
     *
     * @return a Uni emitting the list of all bindings
     */
    public Uni<List<Binding>> listBindings() {
        return Uni.createFrom().item(() -> (List<Binding>)(List<?>)Binding.listAll())
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }
}
