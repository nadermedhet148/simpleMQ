package io.dist.model;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Persistent entity that stores Raft consensus metadata for a cluster node.
 *
 * <p>Each node in the simpleMQ cluster persists its Raft state (current term,
 * voted-for candidate, and last applied log index/term) to SQLite so that it
 * can recover its position in the replicated log after a restart.</p>
 *
 * <p>This metadata is updated in two places:</p>
 * <ul>
 *   <li>{@link io.dist.cluster.SimpleStateMachine} – updates
 *       {@link #lastAppliedIndex} and {@link #lastAppliedTerm} after each
 *       committed log entry is applied.</li>
 *   <li>{@link io.dist.cluster.RaftService} – periodically syncs
 *       {@link #currentTerm} and {@link #votedFor} from the Raft server.</li>
 * </ul>
 *
 * @see io.dist.cluster.RaftService
 * @see io.dist.cluster.SimpleStateMachine
 */
@Entity
@Table(name = "raft_metadata")
public class RaftMetadata extends PanacheEntityBase {

    /** Unique identifier of the cluster node this metadata belongs to. */
    @Id
    public String nodeId;

    /** The latest Raft term this node is aware of. */
    public Long currentTerm;

    /** The candidate this node voted for in the current term (may be {@code null}). */
    public String votedFor;

    /** Index of the last log entry applied to the state machine. */
    public Long lastAppliedIndex;

    /** Term of the last log entry applied to the state machine. */
    public Long lastAppliedTerm;

    /** Default no-arg constructor required by JPA / Hibernate. */
    public RaftMetadata() {}

    /**
     * Creates a new metadata record for a cluster node.
     *
     * @param nodeId           the node identifier
     * @param currentTerm      the current Raft term
     * @param votedFor         the candidate voted for (may be {@code null})
     * @param lastAppliedIndex the last applied log index
     * @param lastAppliedTerm  the term of the last applied log entry
     */
    public RaftMetadata(String nodeId, Long currentTerm, String votedFor, Long lastAppliedIndex, Long lastAppliedTerm) {
        this.nodeId = nodeId;
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
    }
}
