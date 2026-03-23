package io.dist.model;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import java.time.LocalDateTime;

/**
 * JPA entity storing per-processor aggregation state.
 *
 * <p>Each row represents the aggregation state for one processor and one
 * group-by value (or "global" when no grouping is configured). The
 * {@link #stateValue} field is a JSON object with the shape:
 * {@code {"count":0,"sum":0.0,"last":null}}.</p>
 *
 * <p>State updates are replicated through Raft via {@code UPDATE_PROCESSOR_STATE}
 * commands so all cluster nodes stay in sync.</p>
 */
@Entity
@Table(name = "stream_processor_states")
public class StreamProcessorState extends PanacheEntityBase {

    /** Composite key: {@code processorId + ":" + stateKey}. */
    @Id
    public String id;

    @Column(name = "processor_id")
    public String processorId;

    /** The group-by value, or "global" when no grouping is configured. */
    @Column(name = "state_key")
    public String stateKey;

    /** JSON aggregation state: {@code {"count":0,"sum":0.0,"last":null}}. */
    @Column(name = "state_value", length = 2048)
    public String stateValue;

    @Column(name = "updated_at")
    public LocalDateTime updatedAt;

    public StreamProcessorState() {}

    /**
     * Loads an existing state record or creates a new one with default values.
     * Must be called within an active transaction.
     *
     * @param processorId the processor identifier
     * @param stateKey    the group-by value or "global"
     * @return the existing or newly initialised state record
     */
    public static StreamProcessorState getOrCreate(String processorId, String stateKey) {
        String compositeId = processorId + ":" + stateKey;
        StreamProcessorState state = findById(compositeId);
        if (state == null) {
            state = new StreamProcessorState();
            state.id = compositeId;
            state.processorId = processorId;
            state.stateKey = stateKey;
            state.stateValue = "{\"count\":0,\"sum\":0.0,\"last\":null}";
            state.updatedAt = LocalDateTime.now();
        }
        return state;
    }
}
