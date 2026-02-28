package io.dist.service;

import io.dist.model.Binding;
import io.dist.model.Exchange;
import io.dist.model.ExchangeType;
import io.dist.model.Queue;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.util.List;

@ApplicationScoped
public class QueueService {

    @Inject
    io.dist.cluster.RaftService raftService;

    @Transactional
    public void createExchange(String name, ExchangeType type, boolean durable) {
        if (!raftService.isLeader()) {
            throw new RuntimeException("Not the leader");
        }
        boolean success = raftService.replicateCreateExchange(name, type.name(), durable);
        if (!success) {
            throw new RuntimeException("Failed to replicate exchange creation");
        }
    }

    @Transactional
    public void createExchangeLocal(String name, ExchangeType type, boolean durable) {
        if (Exchange.findById(name) == null) {
            new Exchange(name, type, durable).persist();
        }
    }

    @Transactional
    public void deleteExchange(String name) {
        if (!raftService.isLeader()) {
            throw new RuntimeException("Not the leader");
        }
        boolean success = raftService.replicateDeleteExchange(name);
        if (!success) {
            throw new RuntimeException("Failed to replicate exchange deletion");
        }
    }

    @Transactional
    public void deleteExchangeLocal(String name) {
        Exchange.deleteById(name);
        Binding.delete("exchangeName", name);
    }

    @Transactional
    public void createQueue(String name, String group, boolean durable, boolean autoDelete) {
        if (!raftService.isLeader()) {
            throw new RuntimeException("Not the leader");
        }
        boolean success = raftService.replicateCreateQueue(name, group, durable, autoDelete);
        if (!success) {
            throw new RuntimeException("Failed to replicate queue creation");
        }
    }

    @Transactional
    public void createQueueLocal(String name, String group, boolean durable, boolean autoDelete) {
        if (Queue.findById(name) == null) {
            new Queue(name, group, durable, autoDelete).persist();
        }
    }

    @Transactional
    public void deleteQueue(String name) {
        if (!raftService.isLeader()) {
            throw new RuntimeException("Not the leader");
        }
        boolean success = raftService.replicateDeleteQueue(name);
        if (!success) {
            throw new RuntimeException("Failed to replicate queue deletion");
        }
    }

    @Transactional
    public void deleteQueueLocal(String name) {
        Queue.deleteById(name);
        Binding.delete("queueName", name);
    }

    @Transactional
    public void bind(String exchangeName, String queueName, String routingKey) {
        if (!raftService.isLeader()) {
            throw new RuntimeException("Not the leader");
        }
        boolean success = raftService.replicateBind(exchangeName, queueName, routingKey);
        if (!success) {
            throw new RuntimeException("Failed to replicate binding");
        }
    }

    @Transactional
    public void bindLocal(String exchangeName, String queueName, String routingKey) {
        if (Binding.find("exchangeName = ?1 and queueName = ?2 and (routingKey = ?3 or routingKey is null and ?3 is null or ?3 = '')", exchangeName, queueName, routingKey).count() == 0) {
            Binding binding = new Binding(exchangeName, queueName, (routingKey == null || routingKey.isEmpty()) ? null : routingKey);
            binding.persist();
        }
    }

    @Transactional
    public void unbind(String exchangeName, String queueName, String routingKey) {
        if (!raftService.isLeader()) {
            throw new RuntimeException("Not the leader");
        }
        boolean success = raftService.replicateUnbind(exchangeName, queueName, routingKey);
        if (!success) {
            throw new RuntimeException("Failed to replicate unbinding");
        }
    }

    @Transactional
    public void unbindLocal(String exchangeName, String queueName, String routingKey) {
        Binding.delete("exchangeName = ?1 and queueName = ?2 and (routingKey = ?3 or (routingKey is null and (?3 is null or ?3 = '')))", exchangeName, queueName, routingKey);
    }

    public List<Queue> listQueues() {
        return Queue.listAll();
    }
    
    public List<Queue> listQueuesInGroup(String group) {
        return Queue.find("queueGroup", group).list();
    }

    public List<Exchange> listExchanges() {
        return Exchange.listAll();
    }

    public List<Binding> listBindings() {
        return Binding.listAll();
    }
}
