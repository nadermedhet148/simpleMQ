package io.dist.model;

/**
 * Represents the lifecycle status of a {@link Message} within simpleMQ.
 *
 * <p>A message transitions through these states as it moves from publication
 * to final acknowledgement or dead-letter routing.</p>
 */
public enum MessageStatus {

    /** The message has been published and is waiting in the queue to be consumed. */
    PENDING,

    /**
     * The message has been delivered (polled) to a consumer and is awaiting
     * an ACK or NACK. If neither arrives within the visibility timeout the
     * message will be automatically requeued.
     */
    DELIVERED,

    /** The consumer successfully processed the message and acknowledged it. */
    ACKED,

    /** The consumer explicitly rejected (negative-acknowledged) the message. */
    NACKED,

    /**
     * The message exceeded the maximum delivery attempts and was moved to
     * the Dead Letter Queue (DLQ) for manual inspection or reprocessing.
     */
    DLQ
}
