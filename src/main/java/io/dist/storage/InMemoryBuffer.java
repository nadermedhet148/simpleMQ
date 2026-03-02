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

public class InMemoryBuffer {
    private final Queue<Message> messages = new ConcurrentLinkedQueue<>();
    private final Set<String> messageIds = ConcurrentHashMap.newKeySet();

    /** Tracks in-flight messages: messageId → (Message, deliveredAtEpochMillis) */
    private final Map<String, InFlightEntry> inFlight = new ConcurrentHashMap<>();

    public static class InFlightEntry {
        public final Message message;
        public final Instant deliveredAt;

        public InFlightEntry(Message message, Instant deliveredAt) {
            this.message = message;
            this.deliveredAt = deliveredAt;
        }
    }

    public void enqueue(Message message) {
        if (message.id != null) {
            if (messageIds.add(message.id)) {
                messages.offer(message);
            }
        } else {
            messages.offer(message);
        }
    }

    public Message dequeue() {
        Message msg = messages.poll();
        if (msg != null && msg.id != null) {
            messageIds.remove(msg.id);
            inFlight.put(msg.id, new InFlightEntry(msg, Instant.now()));
        }
        return msg;
    }

    public void dequeueById(String id) {
        if (id == null) return;
        messages.removeIf(m -> m.id != null && m.id.equals(id));
        messageIds.remove(id);
    }

    /** Remove a message from in-flight tracking (called on ack/nack). */
    public void removeFromInFlight(String messageId) {
        if (messageId != null) {
            inFlight.remove(messageId);
        }
    }

    /**
     * Returns and removes all in-flight messages that have exceeded the given timeout.
     * These messages should be requeued for redelivery.
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

    public int inFlightSize() {
        return inFlight.size();
    }

    public int size() {
        return messages.size();
    }

    public boolean isEmpty() {
        return messages.isEmpty();
    }
}
