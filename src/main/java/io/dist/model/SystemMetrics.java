package io.dist.model;

import java.util.concurrent.atomic.AtomicLong;

public class SystemMetrics {
    public final AtomicLong totalMessagesPublished = new AtomicLong(0);
    public final AtomicLong totalMessagesAcked = new AtomicLong(0);
    public final AtomicLong totalMessagesNacked = new AtomicLong(0);
    public final AtomicLong totalMessagesDLQ = new AtomicLong(0);
    public final AtomicLong totalPollRequests = new AtomicLong(0);

    // Getters for JSON serialization (Jackson)
    public long getTotalMessagesPublished() { return totalMessagesPublished.get(); }
    public long getTotalMessagesAcked() { return totalMessagesAcked.get(); }
    public long getTotalMessagesNacked() { return totalMessagesNacked.get(); }
    public long getTotalMessagesDLQ() { return totalMessagesDLQ.get(); }
    public long getTotalPollRequests() { return totalPollRequests.get(); }
}
