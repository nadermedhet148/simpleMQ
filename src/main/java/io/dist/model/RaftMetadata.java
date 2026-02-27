package io.dist.model;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "raft_metadata")
public class RaftMetadata extends PanacheEntityBase {
    @Id
    public String nodeId;
    public Long currentTerm;
    public String votedFor;
    public Long lastAppliedIndex;
    public Long lastAppliedTerm;

    public RaftMetadata() {}

    public RaftMetadata(String nodeId, Long currentTerm, String votedFor, Long lastAppliedIndex, Long lastAppliedTerm) {
        this.nodeId = nodeId;
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
    }
}
