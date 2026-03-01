package io.dist.service;

import io.dist.model.Binding;
import io.dist.model.Exchange;
import io.dist.model.ExchangeType;
import io.dist.model.Queue;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.util.List;

@ApplicationScoped
public class QueueService {

    @Inject
    io.dist.cluster.RaftService raftService;

    public Uni<Void> createExchange(String name, ExchangeType type, boolean durable) {
        if (!raftService.isLeader()) {
            return Uni.createFrom().failure(new RuntimeException("Not the leader"));
        }
        return raftService.replicateCreateExchange(name, type.name(), durable)
                .flatMap(success -> {
                    if (!success) return Uni.createFrom().failure(new RuntimeException("Failed to replicate exchange creation"));
                    return Uni.createFrom().voidItem();
                });
    }

    @Transactional
    public void createExchangeLocal(String name, ExchangeType type, boolean durable) {
        if (Exchange.findById(name) == null) {
            new Exchange(name, type, durable).persist();
        }
    }

    public Uni<Void> deleteExchange(String name) {
        if (!raftService.isLeader()) {
            return Uni.createFrom().failure(new RuntimeException("Not the leader"));
        }
        return raftService.replicateDeleteExchange(name)
                .flatMap(success -> {
                    if (!success) return Uni.createFrom().failure(new RuntimeException("Failed to replicate exchange deletion"));
                    return Uni.createFrom().voidItem();
                });
    }

    @Transactional
    public void deleteExchangeLocal(String name) {
        Exchange.deleteById(name);
        Binding.delete("exchangeName", name);
    }

    public Uni<Void> createQueue(String name, String group, boolean durable, boolean autoDelete) {
        if (!raftService.isLeader()) {
            return Uni.createFrom().failure(new RuntimeException("Not the leader"));
        }
        return raftService.replicateCreateQueue(name, group, durable, autoDelete)
                .flatMap(success -> {
                    if (!success) return Uni.createFrom().failure(new RuntimeException("Failed to replicate queue creation"));
                    return Uni.createFrom().voidItem();
                });
    }

    @Transactional
    public void createQueueLocal(String name, String group, boolean durable, boolean autoDelete) {
        if (Queue.findById(name) == null) {
            new Queue(name, group, durable, autoDelete).persist();
        }
    }

    public Uni<Void> deleteQueue(String name) {
        if (!raftService.isLeader()) {
            return Uni.createFrom().failure(new RuntimeException("Not the leader"));
        }
        return raftService.replicateDeleteQueue(name)
                .flatMap(success -> {
                    if (!success) return Uni.createFrom().failure(new RuntimeException("Failed to replicate queue deletion"));
                    return Uni.createFrom().voidItem();
                });
    }

    @Transactional
    public void deleteQueueLocal(String name) {
        Queue.deleteById(name);
        Binding.delete("queueName", name);
    }

    public Uni<Void> bind(String exchangeName, String queueName, String routingKey) {
        if (!raftService.isLeader()) {
            return Uni.createFrom().failure(new RuntimeException("Not the leader"));
        }
        return raftService.replicateBind(exchangeName, queueName, routingKey)
                .flatMap(success -> {
                    if (!success) return Uni.createFrom().failure(new RuntimeException("Failed to replicate binding"));
                    return Uni.createFrom().voidItem();
                });
    }

    @Transactional
    public void bindLocal(String exchangeName, String queueName, String routingKey) {
        if (Binding.find("exchangeName = ?1 and queueName = ?2 and (routingKey = ?3 or routingKey is null and ?3 is null or ?3 = '')", exchangeName, queueName, routingKey).count() == 0) {
            Binding binding = new Binding(exchangeName, queueName, (routingKey == null || routingKey.isEmpty()) ? null : routingKey);
            binding.persist();
        }
    }

    public Uni<Void> unbind(String exchangeName, String queueName, String routingKey) {
        if (!raftService.isLeader()) {
            return Uni.createFrom().failure(new RuntimeException("Not the leader"));
        }
        return raftService.replicateUnbind(exchangeName, queueName, routingKey)
                .flatMap(success -> {
                    if (!success) return Uni.createFrom().failure(new RuntimeException("Failed to replicate unbinding"));
                    return Uni.createFrom().voidItem();
                });
    }

    @Transactional
    public void unbindLocal(String exchangeName, String queueName, String routingKey) {
        Binding.delete("exchangeName = ?1 and queueName = ?2 and (routingKey = ?3 or (routingKey is null and (?3 is null or ?3 = '')))", exchangeName, queueName, routingKey);
    }

    public Uni<List<Queue>> listQueues() {
        return Uni.createFrom().item(() -> (List<Queue>)(List<?>)Queue.listAll())
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }
    
    public Uni<List<Queue>> listQueuesInGroup(String group) {
        return Uni.createFrom().item(() -> (List<Queue>)(List<?>)Queue.find("queueGroup", group).list())
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    public Uni<List<Exchange>> listExchanges() {
        return Uni.createFrom().item(() -> (List<Exchange>)(List<?>)Exchange.listAll())
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    public Uni<List<Binding>> listBindings() {
        return Uni.createFrom().item(() -> (List<Binding>)(List<?>)Binding.listAll())
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }
}
