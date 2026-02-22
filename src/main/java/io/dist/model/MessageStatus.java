package io.dist.model;

public enum MessageStatus {
    PENDING,
    DELIVERED,
    ACKED,
    NACKED,
    DLQ
}
