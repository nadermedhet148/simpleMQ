package io.dist.model;

/**
 * Defines the behavioral type of a queue in simpleMQ.
 *
 * <ul>
 *   <li>{@link #STANDARD} – classic FIFO queue with competing consumers.
 *       Each message is delivered to exactly one consumer and destroyed on ACK.</li>
 *   <li>{@link #STREAM} – append-only log. Multiple independent consumers each
 *       maintain their own read offset; messages are never destroyed on read and
 *       expire only via a configurable TTL.</li>
 * </ul>
 */
public enum QueueType {
    STANDARD,
    STREAM
}
