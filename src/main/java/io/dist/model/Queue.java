package io.dist.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;

/**
 * Persistent entity representing a message queue in simpleMQ.
 *
 * <p>Queues hold messages that have been routed from an {@link Exchange} via
 * a {@link Binding}. Consumers pull messages from a queue using the polling
 * API. Multiple consumers on the same queue compete for messages (each
 * message is delivered to exactly one consumer).</p>
 *
 * <p>Queues can be organized into logical groups via {@link #queueGroup}
 * for management and filtering purposes.</p>
 *
 * @see Exchange
 * @see Binding
 * @see io.dist.storage.InMemoryBuffer
 */
@Entity
@Table(name = "queues")
public class Queue extends PanacheEntityBase {

    /** Unique name identifying this queue. Also serves as the primary key. */
    @Id
    public String name;

    /**
     * Logical group this queue belongs to.
     * Field is named {@code queueGroup} instead of {@code group} to avoid
     * SQL reserved keyword conflicts.
     */
    public String queueGroup;

    /** Whether this queue survives broker restarts (persisted to SQLite). */
    public boolean durable;

    /** Whether this queue is automatically deleted when the last consumer disconnects. */
    public boolean autoDelete;

    /**
     * The behavioral type of this queue. Defaults to {@link QueueType#STANDARD}.
     * A {@link QueueType#STREAM} queue acts as an append-only log where each
     * consumer maintains its own independent read offset.
     */
    @Column(name = "queue_type")
    @Enumerated(EnumType.STRING)
    public QueueType queueType = QueueType.STANDARD;

    /** Default no-arg constructor required by JPA / Hibernate. */
    public Queue() {}

    /**
     * Creates a new queue with the given properties.
     *
     * @param name       unique queue name
     * @param queueGroup logical group for this queue
     * @param durable    whether the queue is persisted to disk
     * @param autoDelete whether the queue auto-deletes when unused
     */
    public Queue(String name, String queueGroup, boolean durable, boolean autoDelete) {
        this.name = name;
        this.queueGroup = queueGroup;
        this.durable = durable;
        this.autoDelete = autoDelete;
        this.queueType = QueueType.STANDARD;
    }

    public Queue(String name, String queueGroup, boolean durable, boolean autoDelete, QueueType queueType) {
        this.name = name;
        this.queueGroup = queueGroup;
        this.durable = durable;
        this.autoDelete = autoDelete;
        this.queueType = queueType != null ? queueType : QueueType.STANDARD;
    }
}
