package io.dist.storage;

import io.dist.model.Message;
import io.dist.model.MessageStatus;
import io.dist.model.Queue;
import io.dist.model.QueueType;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.util.Comparator;
import java.util.List;
import org.jboss.logging.Logger;

/**
 * Manages message persistence to SQLite and recovery on broker startup.
 *
 * <p>On application start this manager queries the database for any messages
 * that were not fully processed (i.e. not {@code ACKED} or {@code DLQ}) and
 * re-enqueues them into the corresponding {@link InMemoryBuffer}s. Messages
 * that were in {@code DELIVERED} state (polled but never ACKed/NACKed) are
 * reset to {@code PENDING} so they can be redelivered.</p>
 *
 * <p>Persistence can be disabled via the {@code simplemq.persistence.enabled}
 * configuration property (defaults to {@code true}). When disabled the broker
 * runs in ephemeral mode and skips recovery on startup.</p>
 *
 * @see StorageService
 * @see io.dist.service.MessagingEngine
 */
@ApplicationScoped
public class PersistenceManager {
    private static final Logger LOG = Logger.getLogger(PersistenceManager.class);

    @Inject
    StorageService storageService;

    @org.eclipse.microprofile.config.inject.ConfigProperty(name = "simplemq.persistence.enabled", defaultValue = "true")
    boolean persistenceEnabled;

    /**
     * Startup hook: recovers unprocessed messages from the database when
     * persistence is enabled. Triggered automatically by Quarkus on application start.
     *
     * @param ev the startup event
     */
    void onStart(@Observes StartupEvent ev) {
        if (persistenceEnabled) {
            LOG.info("Broker starting, recovering messages from database...");
            recoverMessages();
        } else {
            LOG.info("Broker starting in ephemeral mode, skipping recovery.");
        }
    }

    /**
     * Returns whether disk persistence is enabled for this broker instance.
     *
     * @return {@code true} if persistence is enabled
     */
    public boolean isPersistenceEnabled() {
        return persistenceEnabled;
    }

    /**
     * Queries the database for all messages that are not yet {@code ACKED} or
     * in the {@code DLQ}, and re-enqueues them into their respective in-memory
     * buffers. Messages with {@code DELIVERED} status are reset to
     * {@code PENDING} since the original consumer is assumed to be gone after
     * a restart.
     */
    @Transactional
    public void recoverMessages() {
        try {
            List<Message> messages = Message.list("status != ?1 and status != ?2", MessageStatus.ACKED, MessageStatus.DLQ);
            LOG.info("Found " + messages.size() + " messages in database for recovery.");
            for (Message message : messages) {
                Queue queue = Queue.findById(message.queueName);
                if (queue != null && queue.queueType == QueueType.STREAM) {
                    // Stream messages are recovered separately below (ordered by offset)
                    continue;
                }
                // Reset DELIVERED → PENDING since the consumer that held the message
                // is no longer connected after a broker restart.
                if (message.status == MessageStatus.DELIVERED) {
                    message.status = MessageStatus.PENDING;
                }
                storageService.getBuffer(message.queueName).enqueue(message);
            }

            // Recover STREAM queue messages: reload into StreamBuffers ordered by stream_offset
            List<Queue> streamQueues = Queue.list("queueType", QueueType.STREAM);
            for (Queue streamQueue : streamQueues) {
                List<Message> streamMessages = Message.list("queueName = ?1 and status != ?2 and status != ?3 and streamOffset is not null",
                        streamQueue.name, MessageStatus.ACKED, MessageStatus.DLQ);
                streamMessages.sort(Comparator.comparingLong(m -> m.streamOffset));
                StreamBuffer buf = storageService.getStreamBuffer(streamQueue.name);
                for (Message msg : streamMessages) {
                    buf.recoverEnqueue(msg);
                }
                LOG.infof("Recovered %d messages into stream buffer for queue '%s'", streamMessages.size(), streamQueue.name);
            }
        } catch (Exception e) {
            LOG.error("Failed to recover messages from database", e);
        }
    }

    /**
     * Persists a message to the SQLite database.
     * If a message with the same ID already exists the call is silently skipped
     * (idempotent save).
     *
     * @param message the message to persist
     */
    @Transactional
    public void saveMessage(Message message) {
        if (Message.findById(message.id) == null) {
            message.persist();
        } else {
            LOG.infof("Message %s already exists in database, skipping persist", message.id);
        }
    }

    /**
     * Removes a message from the database by its ID.
     *
     * @param messageId the ID of the message to delete
     */
    @Transactional
    public void removeMessage(String messageId) {
        Message.deleteById(messageId);
    }
}
