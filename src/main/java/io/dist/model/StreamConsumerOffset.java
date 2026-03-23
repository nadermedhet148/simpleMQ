package io.dist.model;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Persistent entity tracking a single consumer's read position in a STREAM queue.
 *
 * <p>Each {@code (consumerId, queueName)} pair has at most one row. The
 * {@link #nextOffset} field records the index of the next message the consumer
 * should receive. A value of {@code 0} means the consumer starts from the
 * beginning of the stream (full history).</p>
 *
 * <p>Offsets are replicated through Raft (via
 * {@code UPDATE_STREAM_OFFSET} commands) so they survive node failures and
 * leader elections.</p>
 */
@Entity
@Table(name = "stream_consumer_offsets")
public class StreamConsumerOffset extends PanacheEntityBase {

    /** Composite primary key: {@code consumerId + ":" + queueName}. */
    @Id
    public String id;

    @Column(name = "consumer_id")
    public String consumerId;

    @Column(name = "queue_name")
    public String queueName;

    /** Index of the next message this consumer should receive (0-based, absolute). */
    @Column(name = "next_offset")
    public long nextOffset = 0;

    /** JPA no-arg constructor. */
    public StreamConsumerOffset() {}

    public StreamConsumerOffset(String consumerId, String queueName) {
        this.id = consumerId + ":" + queueName;
        this.consumerId = consumerId;
        this.queueName = queueName;
        this.nextOffset = 0;
    }

    /**
     * Loads an existing offset record or creates a new one starting at 0.
     * Must be called within an active transaction.
     *
     * @param consumerId the consumer identifier
     * @param queueName  the stream queue name
     * @return the existing or newly created offset record
     */
    public static StreamConsumerOffset getOrCreate(String consumerId, String queueName) {
        String compositeId = consumerId + ":" + queueName;
        StreamConsumerOffset offset = findById(compositeId);
        if (offset == null) {
            offset = new StreamConsumerOffset(consumerId, queueName);
        }
        return offset;
    }
}
