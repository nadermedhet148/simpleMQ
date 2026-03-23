package io.dist.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.CollectionTable;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.MapKeyColumn;
import jakarta.persistence.Column;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Persistent entity representing a single message in simpleMQ.
 *
 * <p>Messages are created when a producer publishes to an exchange and are
 * routed to one or more queues depending on the exchange type and routing key.
 * Each message tracks its own delivery lifecycle via {@link #status},
 * {@link #deliveryCount}, and {@link #deliveredAt}.</p>
 *
 * <p>The message is persisted to SQLite for durability and is also held in an
 * {@link io.dist.storage.InMemoryBuffer} for fast consumer access.</p>
 *
 * @see MessageStatus
 * @see io.dist.storage.InMemoryBuffer
 */
@Entity
@Table(name = "messages")
public class Message extends PanacheEntityBase {

    /** Unique identifier for this message (UUID string). */
    @Id
    public String id;

    /** The message body / content supplied by the producer. */
    public String payload;

    /** Routing key used to determine which queues receive this message. */
    public String routingKey;

    /** Name of the exchange this message was published to. */
    public String exchange;

    /** Name of the queue this message is currently assigned to. */
    public String queueName;

    /** Timestamp when the message was originally created / published. */
    public LocalDateTime timestamp;

    /**
     * Timestamp when the message was last delivered (polled) to a consumer.
     * Used by the visibility timeout mechanism to detect unacknowledged messages.
     * Reset to {@code null} on ACK, NACK, or DLQ routing.
     */
    public LocalDateTime deliveredAt;

    /**
     * Number of times this message has been delivered to a consumer.
     * Incremented on each poll. When this reaches the maximum delivery attempts
     * the message is routed to the Dead Letter Queue.
     */
    public int deliveryCount;

    /** Current lifecycle status of this message. */
    public MessageStatus status;

    /**
     * Position of this message within a STREAM queue's append-only log.
     * {@code null} for messages in STANDARD queues.
     */
    @Column(name = "stream_offset")
    public Long streamOffset;

    /**
     * Optional key-value headers attached to the message.
     * Stored in a separate {@code message_headers} collection table.
     */
    @ElementCollection(fetch = jakarta.persistence.FetchType.EAGER)
    @CollectionTable(name = "message_headers", joinColumns = @JoinColumn(name = "message_id"))
    @MapKeyColumn(name = "header_key")
    @Column(name = "header_value")
    public Map<String, String> headers = new HashMap<>();

    /** Default no-arg constructor required by JPA / Hibernate. */
    public Message() {}

    /**
     * Creates a new message with a generated UUID, current timestamp,
     * zero delivery count, and {@link MessageStatus#PENDING} status.
     *
     * @param payload    the message body
     * @param routingKey the routing key for exchange-based routing
     * @param exchange   the exchange name the message is published to
     * @param queueName  the target queue name (may be {@code null} before routing)
     */
    public Message(String payload, String routingKey, String exchange, String queueName) {
        this.id = UUID.randomUUID().toString();
        this.payload = payload;
        this.routingKey = routingKey;
        this.exchange = exchange;
        this.queueName = queueName;
        this.timestamp = LocalDateTime.now();
        this.deliveryCount = 0;
        this.status = MessageStatus.PENDING;
    }
}
