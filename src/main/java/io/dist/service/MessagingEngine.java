package io.dist.service;

import io.dist.model.Message;
import io.dist.model.MessageStatus;
import io.dist.routing.ExchangeRoutingEngine;
import io.dist.storage.PersistenceManager;
import io.dist.storage.StorageService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.util.ArrayList;
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

    public Uni<Void> publish(String exchange, String routingKey, String payload) {
        // getTargetQueues queries the DB — must run on a worker thread, not the IO thread.
        return Uni.createFrom().item(() -> {
            Message template = new Message(payload, routingKey, exchange, null);
            return routingEngine.getTargetQueues(template);
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .flatMap(targetQueues -> {
            if (targetQueues.isEmpty()) {
                LOG.warnf("No queues matched for exchange %s and routing key %s", exchange, routingKey);
                return Uni.createFrom().voidItem();
            }
            if (!raftService.isLeader()) {
                LOG.warn("This node is not the LEADER. Rejecting publish request.");
                return Uni.createFrom().failure(new RuntimeException(
                        "Not the leader. Please send request to: " + raftService.getLeaderId()));
            }
            List<Uni<Boolean>> replications = new ArrayList<>();
            for (String queueName : targetQueues) {
                Message msg = new Message(payload, routingKey, exchange, queueName);
                replications.add(raftService.replicateMessage(msg));
            }
            return Uni.combine().all().unis(replications).with(results -> {
                for (Object res : results) {
                    if (!(Boolean) res) {
                        throw new RuntimeException("Failed to replicate message to quorum");
                    }
                }
                return null;
            });
        });
    }

    public Uni<Void> acknowledgeMessage(String messageId) {
        if (!raftService.isLeader()) {
            return Uni.createFrom().failure(new RuntimeException("Not the leader"));
        }
        return raftService.replicateAck(messageId).replaceWithVoid();
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

    public Uni<Void> nackMessage(String messageId, boolean requeue) {
        if (!raftService.isLeader()) {
            return Uni.createFrom().failure(new RuntimeException("Not the leader"));
        }
        return raftService.replicateNack(messageId, requeue).replaceWithVoid();
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
    
    public Uni<Message> poll(String queueName) {
        if (!raftService.isLeader()) {
            return Uni.createFrom().failure(new RuntimeException("Not the leader"));
        }
        
        return Uni.createFrom().item(() -> {
            metricsService.incrementPollRequests();
            return storageService.getBuffer(queueName).dequeue();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .flatMap(msg -> {
            if (msg == null) {
                return Uni.createFrom().nullItem();
            }
            return raftService.replicatePoll(queueName, msg.id)
                .flatMap(success -> {
                    // Re-fetch from DB. This is blocking!
                    return Uni.createFrom().item(() -> (Message) Message.findById(msg.id))
                        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
                });
        });
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
