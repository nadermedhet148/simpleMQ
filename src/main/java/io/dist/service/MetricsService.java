package io.dist.service;

import io.dist.model.SystemMetrics;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Application-scoped service that tracks broker runtime metrics.
 *
 * <p>Provides thread-safe increment methods for each metric counter
 * (published, acked, nacked, DLQ, poll requests). The underlying
 * {@link SystemMetrics} object uses {@link java.util.concurrent.atomic.AtomicLong}
 * counters so no external synchronization is needed.</p>
 *
 * <p>Metrics are surfaced to the management dashboard via
 * {@link io.dist.api.ManagementResource#getSummary()}.</p>
 *
 * @see SystemMetrics
 */
@ApplicationScoped
public class MetricsService {

    /** Singleton metrics holder for the lifetime of the broker. */
    private final SystemMetrics metrics = new SystemMetrics();

    /**
     * Returns the current metrics snapshot.
     *
     * @return the system metrics object
     */
    public SystemMetrics getMetrics() {
        return metrics;
    }

    /** Increments the total published messages counter. */
    public void incrementPublished() {
        metrics.totalMessagesPublished.incrementAndGet();
    }

    /** Increments the total acknowledged messages counter. */
    public void incrementAcked() {
        metrics.totalMessagesAcked.incrementAndGet();
    }

    /** Increments the total negatively acknowledged messages counter. */
    public void incrementNacked() {
        metrics.totalMessagesNacked.incrementAndGet();
    }

    /** Increments the total dead-letter queue messages counter. */
    public void incrementDLQ() {
        metrics.totalMessagesDLQ.incrementAndGet();
    }

    /** Increments the total poll requests counter. */
    public void incrementPollRequests() {
        metrics.totalPollRequests.incrementAndGet();
    }
}
