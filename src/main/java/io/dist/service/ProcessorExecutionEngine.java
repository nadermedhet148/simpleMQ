package io.dist.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dist.cluster.RaftService;
import io.dist.model.*;
import io.dist.storage.StorageService;
import io.dist.storage.StreamBuffer;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.List;

/**
 * Background scheduler that executes all RUNNING stream processors on the leader.
 *
 * <p>Every second this engine iterates over all RUNNING processors and for each
 * one reads up to {@link #batchSize} messages from the source STREAM queue
 * starting at the processor's current offset. Messages that pass the filter are
 * forwarded to the target exchange, and the offset is checkpointed after each
 * message via the existing {@code UPDATE_STREAM_OFFSET} Raft command (using the
 * processorId as the consumerId). Aggregation state (if configured) is updated
 * and replicated via {@code UPDATE_PROCESSOR_STATE}.</p>
 *
 * <p>Only the leader executes processors — followers simply return early.</p>
 */
@ApplicationScoped
public class ProcessorExecutionEngine {

    private static final Logger LOG = Logger.getLogger(ProcessorExecutionEngine.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @ConfigProperty(name = "simplemq.processor.batch-size", defaultValue = "100")
    int batchSize;

    @Inject
    RaftService raftService;

    @Inject
    StorageService storageService;

    @Inject
    MessagingEngine messagingEngine;

    @Inject
    FilterEvaluator filterEvaluator;

    @Inject
    ProcessorService processorService;

    /**
     * Runs once per second. Skips processing on non-leader nodes.
     */
    @Scheduled(every = "1s", identity = "stream-processor-runner")
    void runProcessors() {
        if (!raftService.isLeader()) return;

        List<StreamProcessor> running = QuarkusTransaction.requiringNew().call(
                () -> StreamProcessor.findAllByStatus(ProcessorStatus.RUNNING)
        );

        for (StreamProcessor p : running) {
            try {
                runProcessor(p);
            } catch (Exception e) {
                LOG.errorf(e, "Error running processor %s (%s)", p.name, p.id);
            }
        }
    }

    private void runProcessor(StreamProcessor p) {
        StreamBuffer buf = storageService.getStreamBuffer(p.sourceQueue);

        long currentOffset = QuarkusTransaction.requiringNew().call(
                () -> StreamConsumerOffset.getOrCreate(p.id, p.sourceQueue).nextOffset
        );

        int processed = 0;
        while (processed < batchSize) {
            Message msg = buf.peekAt(currentOffset);
            if (msg == null) break;

            boolean passes = (p.filterExpression == null)
                    || filterEvaluator.evaluate(p.filterExpression, msg);

            if (passes) {
                // Publish matching message to target exchange
                try {
                    messagingEngine.publish(p.targetExchange, p.targetRoutingKey, msg.payload)
                            .await().indefinitely();
                } catch (Exception e) {
                    LOG.errorf(e, "Processor %s failed to publish message at offset %d", p.id, currentOffset);
                    break; // stop processing this batch on publish failure
                }

                // Update aggregation state if configured
                if (p.aggregationType != null) {
                    try {
                        updateAggregation(p, msg);
                    } catch (Exception e) {
                        LOG.warnf(e, "Processor %s failed to update aggregation state", p.id);
                    }
                }
            }

            // Checkpoint offset after each message (at-least-once delivery)
            raftService.replicateUpdateStreamOffset(p.id, p.sourceQueue, currentOffset + 1)
                    .await().indefinitely();
            currentOffset++;
            processed++;
        }
    }

    private void updateAggregation(StreamProcessor p, Message msg) {
        // Determine the state key from the groupBy field or fall back to "global"
        String stateKey = "global";
        if (p.aggregationGroupBy != null) {
            String groupValue = null;
            if (p.aggregationGroupBy.startsWith("payload.")) {
                String path = p.aggregationGroupBy.substring("payload.".length());
                groupValue = filterEvaluator.resolveJsonPath(path, msg.payload);
            }
            if (groupValue != null) {
                stateKey = groupValue;
            }
        }

        final String finalStateKey = stateKey;

        // Load current state
        String currentStateJson = QuarkusTransaction.requiringNew().call(() -> {
            StreamProcessorState s = StreamProcessorState.getOrCreate(p.id, finalStateKey);
            return s.stateValue;
        });

        // Compute new state
        String newStateJson = computeNewState(p, msg, currentStateJson);

        // Replicate updated state
        raftService.replicateUpdateProcessorState(p.id, finalStateKey, newStateJson)
                .await().indefinitely();
    }

    private String computeNewState(StreamProcessor p, Message msg, String currentStateJson) {
        try {
            ObjectNode state = (ObjectNode) MAPPER.readTree(currentStateJson);
            if (state == null) {
                state = MAPPER.createObjectNode();
                state.put("count", 0);
                state.put("sum", 0.0);
                state.putNull("last");
            }

            switch (p.aggregationType) {
                case COUNT:
                    state.put("count", state.path("count").asLong(0) + 1);
                    break;

                case SUM:
                    if (p.aggregationField != null) {
                        double fieldValue = 0.0;
                        if (p.aggregationField.startsWith("payload.")) {
                            String path = p.aggregationField.substring("payload.".length());
                            String rawVal = filterEvaluator.resolveJsonPath(path, msg.payload);
                            if (rawVal != null) {
                                try { fieldValue = Double.parseDouble(rawVal); } catch (NumberFormatException ignored) {}
                            }
                        }
                        state.put("sum", state.path("sum").asDouble(0.0) + fieldValue);
                    }
                    break;

                case LAST:
                    if (p.aggregationField != null) {
                        String lastValue = null;
                        if (p.aggregationField.startsWith("payload.")) {
                            String path = p.aggregationField.substring("payload.".length());
                            lastValue = filterEvaluator.resolveJsonPath(path, msg.payload);
                        }
                        if (lastValue != null) {
                            state.put("last", lastValue);
                        } else {
                            state.putNull("last");
                        }
                    }
                    break;
            }

            return MAPPER.writeValueAsString(state);
        } catch (Exception e) {
            LOG.warnf(e, "Failed to compute aggregation state for processor %s", p.id);
            return currentStateJson;
        }
    }
}
