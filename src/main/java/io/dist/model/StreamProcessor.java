package io.dist.model;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.List;

/**
 * JPA entity representing a server-side stream processing pipeline definition.
 *
 * <p>A processor continuously reads from a source STREAM queue, evaluates a
 * filter expression on each message, and forwards matching messages to a target
 * exchange. Optionally it maintains stateful aggregations (COUNT/SUM/LAST) that
 * are persisted in {@link StreamProcessorState}.</p>
 *
 * <p>Processor definitions are replicated through Raft so every node in the
 * cluster shares the same set of processors. Only the leader executes them.</p>
 */
@Entity
@Table(name = "stream_processors")
public class StreamProcessor extends PanacheEntityBase {

    /** Unique identifier (UUID string). */
    @Id
    public String id;

    /** Human-readable unique name. */
    @Column(unique = true)
    public String name;

    /** Name of the source STREAM queue to read from. */
    @Column(name = "source_queue")
    public String sourceQueue;

    /**
     * Optional filter expression, e.g. {@code payload.type == "order"}.
     * {@code null} means pass-all.
     */
    @Column(name = "filter_expression")
    public String filterExpression;

    /** Target exchange where matching messages are forwarded. */
    @Column(name = "target_exchange")
    public String targetExchange;

    /** Optional routing key for the target exchange. */
    @Column(name = "target_routing_key")
    public String targetRoutingKey;

    /** Current lifecycle status of this processor. */
    @Enumerated(EnumType.STRING)
    public ProcessorStatus status = ProcessorStatus.RUNNING;

    /** Optional aggregation type (COUNT, SUM, or LAST). */
    @Enumerated(EnumType.STRING)
    @Column(name = "aggregation_type")
    public AggregationType aggregationType;

    /** JSON path used to extract the numeric/string field for SUM or LAST aggregation. */
    @Column(name = "aggregation_field")
    public String aggregationField;

    /** JSON path used to group aggregation state (null = single "global" group). */
    @Column(name = "aggregation_group_by")
    public String aggregationGroupBy;

    /** Timestamp when this processor was created. */
    @Column(name = "created_at")
    public LocalDateTime createdAt;

    public StreamProcessor() {}

    /**
     * Returns all processors with the given status.
     *
     * @param status the desired processor status
     * @return list of matching processors
     */
    public static List<StreamProcessor> findAllByStatus(ProcessorStatus status) {
        return list("status", status);
    }
}
