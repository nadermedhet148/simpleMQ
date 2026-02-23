package io.dist.service;

import io.dist.model.Binding;
import io.dist.model.Exchange;
import io.dist.model.ExchangeType;
import io.dist.model.Queue;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import java.util.List;

@ApplicationScoped
public class QueueService {

    @Transactional
    public void createExchange(String name, ExchangeType type, boolean durable) {
        if (Exchange.findById(name) == null) {
            new Exchange(name, type, durable).persist();
        }
    }

    @Transactional
    public void deleteExchange(String name) {
        Exchange.deleteById(name);
        Binding.delete("exchangeName", name);
    }

    @Transactional
    public void createQueue(String name, String group, boolean durable, boolean autoDelete) {
        if (Queue.findById(name) == null) {
            new Queue(name, group, durable, autoDelete).persist();
        }
    }

    @Transactional
    public void deleteQueue(String name) {
        Queue.deleteById(name);
        Binding.delete("queueName", name);
    }

    @Transactional
    public void bind(String exchangeName, String queueName, String routingKey) {
        if (Binding.find("exchangeName = ?1 and queueName = ?2 and routingKey = ?3", exchangeName, queueName, routingKey).count() == 0) {
            Binding binding = new Binding(exchangeName, queueName, routingKey);
            binding.persist();
        }
    }

    @Transactional
    public void unbind(String exchangeName, String queueName, String routingKey) {
        Binding.delete("exchangeName = ?1 and queueName = ?2 and (routingKey = ?3 or routingKey is null and ?3 is null)", exchangeName, queueName, routingKey);
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
}
