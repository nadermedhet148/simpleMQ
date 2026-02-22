package io.dist.service;

import io.dist.model.Message;
import io.dist.model.MessageStatus;
import io.dist.routing.ExchangeRoutingEngine;
import io.dist.storage.PersistenceManager;
import io.dist.storage.StorageService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.util.List;
import org.jboss.logging.Logger;

@ApplicationScoped
public class MessagingEngine {
    private static final Logger LOG = Logger.getLogger(MessagingEngine.class);
    private static final int MAX_DELIVERY_ATTEMPTS = 3;

    @Inject
    ExchangeRoutingEngine routingEngine;

    @Inject
    StorageService storageService;

    @Inject
    PersistenceManager persistenceManager;

    @Transactional
    public void publish(String exchange, String routingKey, String payload) {
        Message template = new Message(payload, routingKey, exchange, null);
        List<String> targetQueues = routingEngine.getTargetQueues(template);
        
        if (targetQueues.isEmpty()) {
            LOG.warnf("No queues matched for exchange %s and routing key %s", exchange, routingKey);
            return;
        }

        for (String queueName : targetQueues) {
            Message msg = new Message(payload, routingKey, exchange, queueName);
            persistenceManager.saveMessage(msg);
            storageService.getBuffer(queueName).enqueue(msg);
        }
    }

    @Transactional
    public void acknowledgeMessage(String messageId) {
        Message msg = Message.findById(messageId);
        if (msg != null) {
            msg.status = MessageStatus.ACKED;
            LOG.infof("Message %s acknowledged", messageId);
        }
    }

    @Transactional
    public void nackMessage(String messageId, boolean requeue) {
        Message msg = Message.findById(messageId);
        if (msg == null) return;

        if (requeue && msg.deliveryCount < MAX_DELIVERY_ATTEMPTS) {
            msg.status = MessageStatus.PENDING;
            storageService.getBuffer(msg.queueName).enqueue(msg);
            LOG.infof("Message %s nacked and requeued", messageId);
        } else {
            routeToDLQ(msg);
        }
    }

    @Transactional
    public void routeToDLQ(Message msg) {
        msg.status = MessageStatus.DLQ;
        String dlqName = "DLQ." + (msg.queueName != null ? msg.queueName : "unknown");
        msg.queueName = dlqName;
        storageService.getBuffer(dlqName).enqueue(msg);
        LOG.infof("Message %s moved to DLQ %s", msg.id, dlqName);
    }
    
    public Message poll(String queueName) {
        Message msg = storageService.getBuffer(queueName).dequeue();
        if (msg != null) {
            markAsDelivered(msg.id);
            return Message.findById(msg.id);
        }
        return null;
    }

    @Transactional
    public void markAsDelivered(String messageId) {
        Message msg = Message.findById(messageId);
        if (msg != null) {
            msg.status = MessageStatus.DELIVERED;
            msg.deliveryCount++;
        }
    }
}
