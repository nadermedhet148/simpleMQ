package io.dist.storage;

import io.dist.model.Message;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Thread-safe in-memory buffer for a single message queue.
 *
 * <p>Each queue in simpleMQ has its own {@code InMemoryBuffer} that provides
 * fast, lock-free message storage using a {@link ConcurrentLinkedQueue}.
 * Messages are enqueued when published (or requeued after NACK / visibility
 * timeout) and dequeued when a consumer polls.</p>
 *
 * <h3>Deduplication</h3>
 * <p>A {@link ConcurrentHashMap}-backed set of message IDs prevents the same
 * message from being enqueued more than once (e.g. during crash recovery).</p>
 *
 * <h3>In-Flight Tracking</h3>
 * <p>When a message is dequeued it is moved into an <em>in-flight</em> map
 * that records the delivery timestamp. This allows the visibility timeout
 * scheduler to detect messages that were delivered but never ACKed or NACKed,
 * and automatically requeue them so another consumer can pick them up.</p>
 *
 * @see io.dist.storage.StorageService
 * @see io.dist.service.MessagingEngine#checkVisibilityTimeouts()
 */
public class InMemoryBuffer {

    /** Main FIFO queue holding messages available for consumption. */
    private final Queue<Message> messages = new ConcurrentLinkedQueue<>();

    /** Set of message IDs currently in the buffer, used for deduplication on enqueue. */
    private final Set<String> messageIds = ConcurrentHashMap.newKeySet();

    /**
     * Tracks messages that have been dequeued (delivered) but not yet ACKed or NACKed.
     * Key: messageId → Value: {@link InFlightEntry} containing the message and delivery timestamp.
     */
    private final Map<String, InFlightEntry> inFlight = new ConcurrentHashMap<>();

    /**
     * Represents a single in-flight message along with the instant it was delivered.
     * Used by the visibility timeout mechanism to determine when a message has
     * been held by a consumer for too long without acknowledgement.
     */
    public static class InFlightEntry {
        /** The delivered message. */
        public final Message message;
        /** The instant the message was dequeued and handed to a consumer. */
        public final Instant deliveredAt;

        public InFlightEntry(Message message, Instant deliveredAt) {
            this.message = message;
            this.deliveredAt = deliveredAt;
        }
    }

    /**
     * Adds a message to the tail of the buffer.
     *
     * <p>If the message has a non-null ID that is already present in the buffer
     * the call is silently ignored (deduplication). Messages without an ID are
     * always accepted.</p>
     *
     * @param message the message to enqueue
     */
    public void enqueue(Message message) {
        if (message.id != null) {
            if (messageIds.add(message.id)) {
                messages.offer(message);
            }
        } else {
            messages.offer(message);
        }
    }

    /**
     * Atomically removes and returns the head message from the buffer.
     *
     * <p>The dequeued message is automatically moved into the in-flight map
     * so the visibility timeout scheduler can track it. Uses
     * {@link ConcurrentLinkedQueue#poll()} which guarantees that only one
     * concurrent consumer receives a given message (competing consumers pattern).</p>
     *
     * @return the next available message, or {@code null} if the buffer is empty
     */
    public Message dequeue() {
        Message msg = messages.poll();
        if (msg != null && msg.id != null) {
            messageIds.remove(msg.id);
            // Track the message as in-flight for visibility timeout detection
            inFlight.put(msg.id, new InFlightEntry(msg, Instant.now()));
        }
        return msg;
    }

    /**
     * Removes a specific message from the buffer by its ID.
     *
     * <p>Used by follower nodes during Raft replication to dequeue the exact
     * message that the leader already dequeued, rather than taking the head.</p>
     *
     * @param id the message ID to remove
     */
    public void dequeueById(String id) {
        if (id == null) return;
        messages.removeIf(m -> m.id != null && m.id.equals(id));
        messageIds.remove(id);
    }

    /**
     * Removes a message from in-flight tracking.
     *
     * <p>Called when a consumer ACKs or NACKs a message, indicating that the
     * visibility timeout no longer needs to monitor it.</p>
     *
     * @param messageId the ID of the message to stop tracking
     */
    public void removeFromInFlight(String messageId) {
        if (messageId != null) {
            inFlight.remove(messageId);
        }
    }

    /**
     * Finds and removes all in-flight messages that have exceeded the given timeout.
     *
     * <p>These expired messages should be requeued for redelivery (or routed to
     * the DLQ if max delivery attempts have been reached).</p>
     *
     * @param timeoutMillis the visibility timeout in milliseconds
     * @return list of messages whose in-flight time exceeded the timeout
     */
    public List<Message> expireInFlight(long timeoutMillis) {
        List<Message> expired = new ArrayList<>();
        Instant cutoff = Instant.now().minusMillis(timeoutMillis);
        var iterator = inFlight.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (entry.getValue().deliveredAt.isBefore(cutoff)) {
                iterator.remove();
                expired.add(entry.getValue().message);
            }
        }
        return expired;
    }

    /**
     * Returns the number of messages currently in-flight (delivered but not yet ACKed/NACKed).
     *
     * @return in-flight message count
     */
    public int inFlightSize() {
        return inFlight.size();
    }

    /**
     * Returns the number of messages currently waiting in the buffer.
     *
     * @return available message count
     */
    public int size() {
        return messages.size();
    }

    /**
     * Checks whether the buffer has no available messages.
     *
     * @return {@code true} if the buffer is empty
     */
    public boolean isEmpty() {
        return messages.isEmpty();
    }
}
