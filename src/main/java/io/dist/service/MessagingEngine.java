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

    @Inject
    io.dist.cluster.RaftService raftService;

    @Inject
    MetricsService metricsService;

    public void publish(String exchange, String routingKey, String payload) {
        Message template = new Message(payload, routingKey, exchange, null);
        List<String> targetQueues = routingEngine.getTargetQueues(template);
        
        if (targetQueues.isEmpty()) {
            LOG.warnf("No queues matched for exchange %s and routing key %s", exchange, routingKey);
            return;
        }

        // Check if we are the leader before accepting publish
        if (!raftService.isLeader()) {
            LOG.warn("This node is not the LEADER. Rejecting publish request.");
            throw new RuntimeException("Not the leader. Please send request to: " + raftService.getLeaderId());
        }

        for (String queueName : targetQueues) {
            Message msg = new Message(payload, routingKey, exchange, queueName);
            
            // Replicate to cluster. This will also handle local persistence and enqueuing via StateMachine
            boolean success = raftService.replicateMessage(msg);
            if (!success) {
                LOG.errorf("Failed to replicate message %s to cluster", msg.id);
                throw new RuntimeException("Failed to replicate message to quorum");
            }
        }
    }

    @Transactional
    public void acknowledgeMessage(String messageId) {
        if (!raftService.isLeader()) {
            throw new RuntimeException("Not the leader");
        }
        raftService.replicateAck(messageId);
    }

    @Transactional
    public void acknowledgeMessageLocal(String messageId) {
        Message msg = Message.findById(messageId);
        if (msg != null) {
            msg.status = MessageStatus.ACKED;
            LOG.infof("Message %s acknowledged", messageId);
            metricsService.incrementAcked();
        }
    }

    @Transactional
    public void nackMessage(String messageId, boolean requeue) {
        if (!raftService.isLeader()) {
            throw new RuntimeException("Not the leader");
        }
        raftService.replicateNack(messageId, requeue);
    }

    @Transactional
    public void nackMessageLocal(String messageId, boolean requeue) {
        Message msg = Message.findById(messageId);
        if (msg == null) return;

        metricsService.incrementNacked();
        if (requeue && msg.deliveryCount < MAX_DELIVERY_ATTEMPTS) {
            msg.status = MessageStatus.PENDING;
            
            // Enqueue a fresh copy to avoid Hibernate session issues when polling
            Message copy = new Message();
            copy.id = msg.id;
            copy.payload = msg.payload;
            copy.routingKey = msg.routingKey;
            copy.exchange = msg.exchange;
            copy.queueName = msg.queueName;
            copy.timestamp = msg.timestamp;
            copy.deliveryCount = msg.deliveryCount;
            copy.status = MessageStatus.PENDING;
            
            storageService.getBuffer(msg.queueName).enqueue(copy);
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
        metricsService.incrementDLQ();
    }
    
    public Message poll(String queueName) {
        if (!raftService.isLeader()) {
            throw new RuntimeException("Not the leader");
        }
        metricsService.incrementPollRequests();
        Message msg = storageService.getBuffer(queueName).dequeue();
        if (msg != null) {
            // Replicate the poll so other nodes also dequeue it and mark it as delivered
            raftService.replicatePoll(queueName, msg.id);
            
            // Re-fetch from DB to get updated delivery count and status
            Message updated = Message.findById(msg.id);
            if (updated != null) {
                return updated;
            }
            return msg;
        }
        return null;
    }

    @Transactional
    public void pollLocal(String queueName, String messageId) {
        // If we are the leader, we already dequeued it in poll(). 
        // If we are a follower, we need to dequeue it now.
        if (!raftService.isLeader()) {
            storageService.getBuffer(queueName).dequeueById(messageId);
        }
        markAsDelivered(messageId);
    }

    @Transactional
    public void markAsDelivered(String messageId) {
        Message.update("status = ?1, deliveryCount = deliveryCount + 1 where id = ?2", 
                MessageStatus.DELIVERED, messageId);
    }
}
