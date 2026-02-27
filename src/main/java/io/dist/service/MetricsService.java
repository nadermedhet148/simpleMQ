package io.dist.service;

import io.dist.model.SystemMetrics;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class MetricsService {
    private final SystemMetrics metrics = new SystemMetrics();

    public SystemMetrics getMetrics() {
        return metrics;
    }

    public void incrementPublished() {
        metrics.totalMessagesPublished.incrementAndGet();
    }

    public void incrementAcked() {
        metrics.totalMessagesAcked.incrementAndGet();
    }

    public void incrementNacked() {
        metrics.totalMessagesNacked.incrementAndGet();
    }

    public void incrementDLQ() {
        metrics.totalMessagesDLQ.incrementAndGet();
    }

    public void incrementPollRequests() {
        metrics.totalPollRequests.incrementAndGet();
    }
}
