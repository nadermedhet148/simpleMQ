package io.dist.service;

import io.dist.model.Message;
import io.dist.model.MessageStatus;
import io.dist.routing.ExchangeRoutingEngine;
import io.dist.storage.InMemoryBuffer;
import io.dist.storage.PersistenceManager;
import io.dist.storage.StorageService;
import io.dist.storage.StreamBuffer;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * Central messaging engine that orchestrates the full message lifecycle:
 * publish → route → enqueue → poll → ack/nack → DLQ.
 *
 * <p>All write operations are replicated through the Raft consensus layer so
 * that every node in the cluster applies the same sequence of state changes.
 * Read-heavy operations (poll) also go through Raft to guarantee consistency.</p>
 *
 * <h3>Visibility Timeout</h3>
 * <p>When a message is polled it enters an <em>in-flight</em> state. If the
 * consumer does not ACK or NACK within the configurable visibility timeout
 * ({@code simplemq.visibility.timeout-ms}, default 30 s) a background
 * scheduler automatically requeues the message so another consumer can pick
 * it up. After {@link #MAX_DELIVERY_ATTEMPTS} failed deliveries the message
 * is routed to the Dead Letter Queue (DLQ).</p>
 *
 * @see io.dist.routing.ExchangeRoutingEngine
 * @see io.dist.storage.StorageService
 * @see io.dist.cluster.RaftService
 */
@ApplicationScoped
public class MessagingEngine {
    private static final Logger LOG = Logger.getLogger(MessagingEngine.class);

    /** Maximum number of times a message can be delivered before being sent to the DLQ. */
    private static final int MAX_DELIVERY_ATTEMPTS = 3;

    /** Resolves target queues for a message based on exchange type and bindings. */
    @Inject
    ExchangeRoutingEngine routingEngine;

    /** Registry of in-memory buffers, one per queue. */
    @Inject
    StorageService storageService;

    /** Handles SQLite persistence for messages. */
    @Inject
    PersistenceManager persistenceManager;

    /** Raft consensus service for replicating commands across the cluster. */
    @Inject
    io.dist.cluster.RaftService raftService;

    /** Tracks runtime metrics (published, acked, nacked, DLQ, poll counts). */
    @Inject
    MetricsService metricsService;

    /**
     * How long (in milliseconds) a delivered message waits for ACK/NACK
     * before being automatically requeued. Configurable via
     * {@code simplemq.visibility.timeout-ms}.
     */
    @ConfigProperty(name = "simplemq.visibility.timeout-ms", defaultValue = "30000")
    long visibilityTimeoutMs;

    /**
     * TTL for messages in STREAM queues. Messages older than this are evicted
     * from the in-memory StreamBuffer by the periodic eviction scheduler.
     * Configurable via {@code simplemq.stream.ttl-ms} (default 24 hours).
     */
    @ConfigProperty(name = "simplemq.stream.ttl-ms", defaultValue = "86400000")
    long streamTtlMs;

    // ======================== Publish ========================

    /**
     * Publishes a message to the specified exchange with the given routing key.
     *
     * <p>The routing engine determines which queues match, then each queue
     * receives its own copy of the message replicated through Raft. Only the
     * leader node can accept publish requests.</p>
     *
     * @param exchange   the target exchange name
     * @param routingKey the routing key for DIRECT exchanges
     * @param payload    the message body
     * @return a Uni that completes when all replications succeed
     */
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
            // Replicate a PUBLISH command for each target queue through Raft
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

    // ======================== Acknowledge ========================

    /**
     * Acknowledges a message by replicating an ACK command through Raft.
     * Only the leader can accept this request.
     *
     * @param messageId the ID of the message to acknowledge
     * @return a Uni that completes when replication succeeds
     */
    public Uni<Void> acknowledgeMessage(String messageId) {
        return raftService.replicateAck(messageId).replaceWithVoid();
    }

    /**
     * Local ACK applied by the state machine on every node.
     * Marks the message as {@link MessageStatus#ACKED}, clears the
     * {@code deliveredAt} timestamp, and removes it from in-flight tracking.
     *
     * @param messageId the ID of the message to acknowledge
     */
    @Transactional
    public void acknowledgeMessageLocal(String messageId) {
        Message msg = Message.findById(messageId);
        if (msg != null) {
            msg.status = MessageStatus.ACKED;
            msg.deliveredAt = null;
            LOG.infof("Message %s acknowledged", messageId);
            metricsService.incrementAcked();
            // Remove from in-flight tracking so the visibility timeout ignores it
            storageService.getBuffer(msg.queueName).removeFromInFlight(messageId);
        }
    }

    // ======================== Negative Acknowledge ========================

    /**
     * Negatively acknowledges a message by replicating a NACK command through Raft.
     * Only the leader can accept this request.
     *
     * @param messageId the ID of the message to nack
     * @param requeue   whether to requeue the message or send it to the DLQ
     * @return a Uni that completes when replication succeeds
     */
    public Uni<Void> nackMessage(String messageId, boolean requeue) {
        return raftService.replicateNack(messageId, requeue).replaceWithVoid();
    }

    /**
     * Local NACK applied by the state machine on every node.
     *
     * <p>If {@code requeue} is {@code true} and the message has not exceeded
     * {@link #MAX_DELIVERY_ATTEMPTS}, it is set back to {@link MessageStatus#PENDING}
     * and re-enqueued into the in-memory buffer. Otherwise it is routed to the DLQ.</p>
     *
     * <p>A detached copy of the message is enqueued to avoid Hibernate session
     * issues when the message is later polled from the buffer.</p>
     *
     * @param messageId the ID of the message to nack
     * @param requeue   whether to requeue or route to DLQ
     */
    @Transactional
    public void nackMessageLocal(String messageId, boolean requeue) {
        Message msg = Message.findById(messageId);
        if (msg == null) return;

        metricsService.incrementNacked();
        // Remove from in-flight tracking
        storageService.getBuffer(msg.queueName).removeFromInFlight(messageId);

        if (requeue && msg.deliveryCount < MAX_DELIVERY_ATTEMPTS) {
            msg.status = MessageStatus.PENDING;
            msg.deliveredAt = null;
            
            // Enqueue a detached copy to avoid Hibernate session issues when polling
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
            // Max attempts reached or requeue not requested — route to DLQ
            routeToDLQ(msg);
        }
    }

    // ======================== Dead Letter Queue ========================

    /**
     * Routes a message to the Dead Letter Queue (DLQ).
     *
     * <p>The DLQ queue name follows the convention {@code DLQ.<originalQueueName>}.
     * The message status is set to {@link MessageStatus#DLQ} and it is enqueued
     * into the DLQ's in-memory buffer for manual inspection or reprocessing.</p>
     *
     * @param msg the message to route to the DLQ (must be a managed entity)
     */
    @Transactional
    public void routeToDLQ(Message msg) {
        msg.status = MessageStatus.DLQ;
        msg.deliveredAt = null;
        String dlqName = "DLQ." + (msg.queueName != null ? msg.queueName : "unknown");
        msg.queueName = dlqName;
        storageService.getBuffer(dlqName).enqueue(msg);
        LOG.infof("Message %s moved to DLQ %s", msg.id, dlqName);
        metricsService.incrementDLQ();
    }

    // ======================== Poll (Consumer Pull) ========================
    
    /**
     * Polls the next available message from the specified queue.
     *
     * <p>The message is atomically dequeued from the in-memory buffer (so no
     * other consumer can receive it) and then a POLL command is replicated
     * through Raft to update the database on all nodes. The returned message
     * is re-fetched from the DB to reflect the updated status.</p>
     *
     * @param queueName the queue to poll from
     * @return a Uni emitting the next message, or {@code null} if the queue is empty
     */
    public Uni<Message> poll(String queueName) {
        return Uni.createFrom().item(() -> {
            metricsService.incrementPollRequests();
            return storageService.getBuffer(queueName).dequeue();
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .flatMap(msg -> {
            if (msg == null) {
                return Uni.createFrom().nullItem();
            }
            // Replicate the poll event so all nodes mark the message as DELIVERED
            return raftService.replicatePoll(queueName, msg.id)
                .flatMap(success -> {
                    // Re-fetch the message from DB to get the updated DELIVERED status
                    // Use a new transaction to ensure we see the committed changes from pollLocal
                    return Uni.createFrom().item(() -> {
                        return io.quarkus.narayana.jta.QuarkusTransaction.requiringNew().call(() -> {
                            return (Message) Message.findById(msg.id);
                        });
                    }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
                });
        });
    }

    /**
     * Local poll applied by the state machine on every node.
     *
     * <p>On the leader the message was already dequeued in {@link #poll(String)}.
     * On follower nodes the message is dequeued here by ID to keep buffers
     * in sync. Then the message is marked as {@link MessageStatus#DELIVERED}.</p>
     *
     * @param queueName the queue the message was polled from
     * @param messageId the ID of the polled message
     */
    @Transactional
    public void pollLocal(String queueName, String messageId) {
        // On followers, dequeue the specific message to keep in-memory state consistent
        if (!raftService.isLeader()) {
            storageService.getBuffer(queueName).dequeueById(messageId);
        }
        markAsDelivered(messageId);
    }

    /**
     * Marks a message as delivered in the database: sets status to
     * {@link MessageStatus#DELIVERED}, increments the delivery count,
     * and records the delivery timestamp for visibility timeout tracking.
     *
     * @param messageId the ID of the message to mark as delivered
     */
    @Transactional
    public void markAsDelivered(String messageId) {
        Message msg = Message.findById(messageId);
        if (msg != null) {
            msg.status = MessageStatus.DELIVERED;
            msg.deliveryCount = msg.deliveryCount + 1;
            msg.deliveredAt = LocalDateTime.now();
        }
    }

    // ======================== Visibility Timeout Scheduler ========================

    /**
     * Scheduled background task that checks all in-flight messages across all queues.
     *
     * <p>Runs every 5 seconds on the leader node only. Messages that have
     * exceeded the visibility timeout without being ACKed or NACKed are
     * automatically requeued so another consumer can pick them up. If the
     * message has already reached {@link #MAX_DELIVERY_ATTEMPTS} it is
     * routed to the DLQ instead.</p>
     */
    @Scheduled(every = "5s", identity = "visibility-timeout-checker")
    public void checkVisibilityTimeouts() {
        // Only the leader should manage visibility timeouts
        if (!raftService.isLeader()) {
            return;
        }
        for (Map.Entry<String, InMemoryBuffer> entry : storageService.getAllBuffers().entrySet()) {
            String queueName = entry.getKey();
            InMemoryBuffer buffer = entry.getValue();
            // Collect all messages that have been in-flight longer than the timeout
            List<Message> expired = buffer.expireInFlight(visibilityTimeoutMs);
            for (Message msg : expired) {
                LOG.infof("Message %s in queue %s exceeded visibility timeout, requeuing", msg.id, queueName);
                redeliverExpiredMessage(msg, queueName);
            }
        }
    }

    // ======================== Stream TTL Eviction ========================

    /**
     * Scheduled background task that evicts expired messages from all STREAM queue
     * buffers. Runs every 60 seconds on the leader node only.
     *
     * <p>Messages whose {@code timestamp} is older than {@link #streamTtlMs} are
     * removed from the front of each {@link StreamBuffer}. Consumer offsets that
     * fall below the new base offset will simply receive {@code null} from
     * {@link StreamBuffer#peekAt} and should be treated as "no message available".</p>
     */
    @Scheduled(every = "60s", identity = "stream-ttl-eviction")
    public void evictExpiredStreamMessages() {
        if (!raftService.isLeader()) {
            return;
        }
        Instant cutoff = Instant.now().minusMillis(streamTtlMs);
        for (Map.Entry<String, StreamBuffer> entry : storageService.getAllStreamBuffers().entrySet()) {
            entry.getValue().evictExpiredBefore(cutoff);
        }
    }

    /**
     * Redelivers a single expired in-flight message.
     *
     * <p>If the message's delivery count has reached {@link #MAX_DELIVERY_ATTEMPTS}
     * it is routed to the DLQ. Otherwise it is reset to {@link MessageStatus#PENDING}
     * and a detached copy is enqueued back into the in-memory buffer.</p>
     *
     * @param expiredMsg the expired in-flight message (may be stale — DB is re-checked)
     * @param queueName  the queue the message belongs to
     */
    @Transactional
    public void redeliverExpiredMessage(Message expiredMsg, String queueName) {
        // Re-fetch from DB to get the latest state
        Message msg = Message.findById(expiredMsg.id);
        if (msg == null || msg.status != MessageStatus.DELIVERED) {
            return;
        }
        if (msg.deliveryCount >= MAX_DELIVERY_ATTEMPTS) {
            routeToDLQ(msg);
        } else {
            msg.status = MessageStatus.PENDING;
            msg.deliveredAt = null;

            // Enqueue a detached copy to avoid Hibernate session issues
            Message copy = new Message();
            copy.id = msg.id;
            copy.payload = msg.payload;
            copy.routingKey = msg.routingKey;
            copy.exchange = msg.exchange;
            copy.queueName = msg.queueName;
            copy.timestamp = msg.timestamp;
            copy.deliveryCount = msg.deliveryCount;
            copy.status = MessageStatus.PENDING;

            storageService.getBuffer(queueName).enqueue(copy);
            LOG.infof("Message %s requeued after visibility timeout expiry", msg.id);
        }
    }
}
