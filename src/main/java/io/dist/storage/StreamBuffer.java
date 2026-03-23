package io.dist.storage;

import io.dist.model.Message;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Append-only in-memory log for a STREAM queue.
 *
 * <p>Unlike the FIFO {@link InMemoryBuffer}, a {@code StreamBuffer} never
 * removes messages on read. Each message is assigned a monotonically
 * increasing {@link #nextStreamOffset stream offset} when enqueued.
 * Consumers advance their own position independently via
 * {@link io.dist.model.StreamConsumerOffset}.</p>
 *
 * <h3>Offset arithmetic</h3>
 * <p>After TTL eviction, earlier messages are removed from the front of the
 * log. A {@link #baseOffset} is maintained so that absolute consumer offsets
 * continue to map correctly to list indices:
 * {@code index = consumerOffset - baseOffset}.</p>
 *
 * <h3>Thread safety</h3>
 * <p>Reads ({@link #peekAt}) are fully concurrent and lock-free. Writes
 * ({@link #enqueue}) are also concurrent via {@link AtomicLong}.
 * {@link #evictExpiredBefore} should only be called from the leader's TTL
 * eviction scheduler (single goroutine), which is safe.</p>
 */
public class StreamBuffer {

    /** Append-only message log. Index 0 corresponds to {@link #baseOffset}. */
    private final CopyOnWriteArrayList<Message> messages = new CopyOnWriteArrayList<>();

    /** The absolute stream offset that will be assigned to the next enqueued message. */
    private final AtomicLong nextStreamOffset = new AtomicLong(0);

    /**
     * The absolute offset of the first element currently in {@link #messages}.
     * Increases by 1 for each message evicted from the front.
     */
    private volatile long baseOffset = 0;

    /**
     * Appends a message to the end of the log and assigns it a stream offset.
     *
     * @param message the message to append (its {@code streamOffset} field will be set)
     * @return the absolute stream offset assigned to this message
     */
    public long enqueue(Message message) {
        long offset = nextStreamOffset.getAndIncrement();
        message.streamOffset = offset;
        messages.add(message);
        return offset;
    }

    /**
     * Returns the message at the given absolute stream offset without removing it,
     * or {@code null} if the offset is beyond the current end of the log or has
     * already been evicted.
     *
     * @param offset the absolute stream offset
     * @return the message at that offset, or {@code null}
     */
    public Message peekAt(long offset) {
        long index = offset - baseOffset;
        if (index < 0 || index >= messages.size()) {
            return null;
        }
        return messages.get((int) index);
    }

    /**
     * Returns the number of messages currently retained in the log
     * (may be less than {@link #getNextOffset()} after eviction).
     *
     * @return retained message count
     */
    public long size() {
        return messages.size();
    }

    /**
     * Returns the absolute offset that will be assigned to the next enqueued message.
     * Equals {@link #baseOffset} + {@link #size()}.
     *
     * @return next stream offset
     */
    public long getNextOffset() {
        return nextStreamOffset.get();
    }

    /**
     * Returns the absolute offset of the oldest message still in the buffer.
     *
     * @return base offset
     */
    public long getBaseOffset() {
        return baseOffset;
    }

    /**
     * Re-enqueues a message during startup recovery, preserving its original
     * stream offset instead of assigning a new one.
     *
     * <p>Messages must be recovered in ascending {@code streamOffset} order.
     * The first recovered message sets {@link #baseOffset}; subsequent calls
     * advance {@link #nextStreamOffset} past each message's offset.</p>
     *
     * @param message a message loaded from the database (must have {@code streamOffset} set)
     */
    public void recoverEnqueue(Message message) {
        if (message.streamOffset == null) return;
        if (messages.isEmpty()) {
            // First message establishes the base offset after any prior evictions
            baseOffset = message.streamOffset;
        }
        messages.add(message);
        // Advance the next-offset counter past this recovered message
        nextStreamOffset.updateAndGet(cur -> Math.max(cur, message.streamOffset + 1));
    }

    /**
     * Removes messages from the front of the log whose {@code timestamp} is
     * strictly before {@code cutoff}, advancing {@link #baseOffset} accordingly.
     * Should be called only from the leader's TTL eviction scheduler.
     *
     * @param cutoff messages older than this instant are evicted
     */
    public void evictExpiredBefore(Instant cutoff) {
        LocalDateTime cutoffLdt = LocalDateTime.ofInstant(cutoff, ZoneId.systemDefault());
        int removeCount = 0;
        for (Message msg : messages) {
            if (msg.timestamp != null && msg.timestamp.isBefore(cutoffLdt)) {
                removeCount++;
            } else {
                break; // messages are ordered by arrival; stop at first non-expired
            }
        }
        if (removeCount > 0) {
            messages.subList(0, removeCount).clear();
            baseOffset += removeCount;
        }
    }
}
