package io.dist.model;

import java.util.List;
import java.util.Map;

/**
 * Data transfer object (DTO) returned by the management dashboard API.
 *
 * <p>Aggregates the current state of the broker node including cluster
 * information, all registered exchanges and queues, and runtime metrics.
 * Serialized to JSON by the {@code /api/management/summary} endpoint.</p>
 *
 * @see io.dist.api.ManagementResource#getSummary()
 */
public class ManagementSummary {

    /** Identifier of the current cluster node. */
    public String nodeId;

    /** Identifier of the current Raft leader (or "None" if no leader is elected). */
    public String leaderId;

    /** Whether this node is currently the Raft leader. */
    public boolean isLeader;

    /** List of all known cluster peers in {@code "id=address"} format. */
    public List<String> peers;

    /** Snapshot of all registered queues with their current message counts. */
    public List<QueueInfo> queues;

    /** Snapshot of all registered exchanges. */
    public List<Exchange> exchanges;

    /** Current runtime metrics (published, acked, nacked, DLQ counts, etc.). */
    public SystemMetrics metrics;

    /**
     * Lightweight view of a single queue used in the management summary.
     * Contains the queue name, group, in-memory message count, and durability flag.
     */
    public static class QueueInfo {

        /** Queue name. */
        public String name;

        /** Logical group the queue belongs to. */
        public String group;

        /** Number of messages currently waiting in the in-memory buffer. */
        public int messageCount;

        /** Whether the queue is durable (persisted to disk). */
        public boolean durable;

        /**
         * Creates a new QueueInfo snapshot.
         *
         * @param name         queue name
         * @param group        logical group
         * @param messageCount current in-memory message count
         * @param durable      durability flag
         */
        public QueueInfo(String name, String group, int messageCount, boolean durable) {
            this.name = name;
            this.group = group;
            this.messageCount = messageCount;
            this.durable = durable;
        }
    }
}
