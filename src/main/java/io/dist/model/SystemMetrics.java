package io.dist.model;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe runtime metrics for the simpleMQ broker.
 *
 * <p>Counters are backed by {@link AtomicLong} so they can be safely
 * incremented from multiple threads without synchronization. The metrics
 * are exposed via the management summary endpoint and the dashboard UI.</p>
 *
 * @see io.dist.service.MetricsService
 * @see ManagementSummary
 */
public class SystemMetrics {

    /** Total number of messages successfully published to exchanges. */
    public final AtomicLong totalMessagesPublished = new AtomicLong(0);

    /** Total number of messages acknowledged (ACKed) by consumers. */
    public final AtomicLong totalMessagesAcked = new AtomicLong(0);

    /** Total number of messages negatively acknowledged (NACKed) by consumers. */
    public final AtomicLong totalMessagesNacked = new AtomicLong(0);

    /** Total number of messages routed to a Dead Letter Queue (DLQ). */
    public final AtomicLong totalMessagesDLQ = new AtomicLong(0);

    /** Total number of poll (consume) requests received from consumers. */
    public final AtomicLong totalPollRequests = new AtomicLong(0);

    // --- Getters for JSON serialization (Jackson requires getter methods) ---

    public long getTotalMessagesPublished() { return totalMessagesPublished.get(); }
    public long getTotalMessagesAcked() { return totalMessagesAcked.get(); }
    public long getTotalMessagesNacked() { return totalMessagesNacked.get(); }
    public long getTotalMessagesDLQ() { return totalMessagesDLQ.get(); }
    public long getTotalPollRequests() { return totalPollRequests.get(); }
}
