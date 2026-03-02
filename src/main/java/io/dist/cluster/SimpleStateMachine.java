package io.dist.cluster;

import io.dist.model.Message;
import io.dist.model.MessageStatus;
import io.dist.storage.PersistenceManager;
import io.dist.storage.StorageService;
import io.dist.service.MessagingEngine;
import io.dist.service.MetricsService;
import io.dist.model.ExchangeType;
import io.dist.service.QueueService;
import io.dist.model.RaftMetadata;
import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.enterprise.inject.spi.CDI;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Raft state machine implementation for simpleMQ.
 *
 * <p>This class is the core of the replicated state: every committed Raft log
 * entry is applied here on <b>every</b> node in the cluster, ensuring that all
 * nodes converge to the same state. The state machine processes pipe-delimited
 * command strings that represent broker operations.</p>
 *
 * <h3>Supported Commands</h3>
 * <ul>
 *   <li>{@code PUBLISH|id|payload|routingKey|exchange|queue|timestamp} – persist
 *       and enqueue a new message</li>
 *   <li>{@code POLL|queueName|messageId} – mark a message as delivered</li>
 *   <li>{@code ACK|messageId} – acknowledge a message</li>
 *   <li>{@code NACK|messageId|requeue} – negatively acknowledge a message</li>
 *   <li>{@code CREATE_EXCHANGE|name|type|durable} – create an exchange</li>
 *   <li>{@code DELETE_EXCHANGE|name} – delete an exchange</li>
 *   <li>{@code CREATE_QUEUE|name|group|durable|autoDelete} – create a queue</li>
 *   <li>{@code DELETE_QUEUE|name} – delete a queue</li>
 *   <li>{@code BIND|exchange|queue|routingKey} – bind a queue to an exchange</li>
 *   <li>{@code UNBIND|exchange|queue|routingKey} – unbind a queue from an exchange</li>
 * </ul>
 *
 * <h3>Threading</h3>
 * <p>All database writes are serialized through a single-threaded executor
 * ({@code SM_EXECUTOR}) because SQLite only supports one concurrent write
 * transaction. This prevents "database is locked" errors when multiple Raft
 * log entries are applied concurrently.</p>
 *
 * <h3>Recovery</h3>
 * <p>The last applied log index and term are persisted to the
 * {@link RaftMetadata} table after each transaction, allowing the state
 * machine to resume from the correct position after a restart.</p>
 *
 * @see RaftService
 * @see RaftMetadata
 */
public class SimpleStateMachine extends BaseStateMachine {
    private static final Logger LOG = Logger.getLogger(SimpleStateMachine.class);

    /**
     * Single-threaded executor that serializes all state-machine DB writes.
     * Multiple concurrent applyTransaction() calls on the worker pool would
     * cause SQLite "database is locked" errors because SQLite only allows one
     * write transaction at a time.
     */
    private static final java.util.concurrent.ExecutorService SM_EXECUTOR =
        java.util.concurrent.Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "raft-sm-executor");
            t.setDaemon(true);
            return t;
        });

    /** Cached node ID, lazily resolved from RaftService via CDI. */
    private String nodeId;

    public SimpleStateMachine() {
        // Node ID is resolved lazily via CDI because this class is instantiated
        // by the Raft framework (not by CDI), so constructor injection is unavailable.
    }

    /** Lazily resolves the node ID from the CDI-managed RaftService. */
    private String getNodeId() {
        if (nodeId == null) {
            nodeId = CDI.current().select(RaftService.class).get().getNodeId();
        }
        return nodeId;
    }

    /** Obtains the PersistenceManager from CDI. */
    private PersistenceManager getPersistenceManager() {
        return CDI.current().select(PersistenceManager.class).get();
    }

    /** Obtains the StorageService from CDI. */
    private StorageService getStorageService() {
        return CDI.current().select(StorageService.class).get();
    }

    /**
     * Recovers the last applied term index from SQLite on startup.
     * This allows the Raft server to skip already-applied log entries
     * after a restart.
     *
     * @return the last applied term index, or the superclass default if not found
     */
    @Override
    public TermIndex getLastAppliedTermIndex() {
        try {
            return QuarkusTransaction.requiringNew().call(() -> {
                RaftMetadata metadata = RaftMetadata.findById(getNodeId());
                if (metadata != null && metadata.lastAppliedIndex != null && metadata.lastAppliedIndex >= 0) {
                    LOG.infof("Recovered last applied index %d, term %d from SQLite", metadata.lastAppliedIndex, metadata.lastAppliedTerm);
                    return TermIndex.valueOf(metadata.lastAppliedTerm, metadata.lastAppliedIndex);
                }
                return super.getLastAppliedTermIndex();
            });
        } catch (Exception e) {
            LOG.error("Failed to recover last applied index from SQLite", e);
            return super.getLastAppliedTermIndex();
        }
    }

    /** Obtains the QueueService from CDI. */
    private QueueService getQueueService() {
        return CDI.current().select(QueueService.class).get();
    }

    /** Obtains the MessagingEngine from CDI. */
    private MessagingEngine getMessagingEngine() {
        return CDI.current().select(MessagingEngine.class).get();
    }

    /** Obtains the MetricsService from CDI. */
    private MetricsService getMetricsService() {
        return CDI.current().select(MetricsService.class).get();
    }

    /**
     * Applies a committed Raft log entry to the local state.
     *
     * <p>Parses the pipe-delimited command string and delegates to the
     * appropriate service method. After successful application the last
     * applied index and term are persisted to {@link RaftMetadata}.</p>
     *
     * @param trx the transaction context containing the log entry
     * @return a future that completes with a SUCCESS or FAILURE message
     */
    @Override
    public CompletableFuture<org.apache.ratis.protocol.Message> applyTransaction(TransactionContext trx) {
        return CompletableFuture.supplyAsync(() -> {
            final long index = trx.getLogEntry().getIndex();
            final long term = trx.getLogEntry().getTerm();
            final String command = trx.getLogEntry().getStateMachineLogEntry().getLogData().toString(StandardCharsets.UTF_8);
            LOG.infof("Applying transaction: %s at index %d (Thread: %s)", command, index, Thread.currentThread().getName());

            try {
                // Parse the pipe-delimited command
                String[] parts = command.split("\\|", -1);
                String type = parts[0];
                
                // Dispatch to the appropriate handler based on command type
                if (type.equals("PUBLISH")) {
                    Message msg = new Message();
                    msg.id = parts[1];
                    msg.payload = decodeValue(parts[2]);
                    msg.routingKey = decodeValue(parts[3]);
                    msg.exchange = parts[4];
                    msg.queueName = parts[5];
                    msg.timestamp = LocalDateTime.parse(parts[6]);
                    msg.deliveryCount = 0;
                    msg.status = MessageStatus.PENDING;

                    LOG.infof("Processing message %s for queue %s", msg.id, msg.queueName);
                    getPersistenceManager().saveMessage(msg);
                    getStorageService().getBuffer(msg.queueName).enqueue(msg);
                    getMetricsService().incrementPublished();
                } else if (type.equals("POLL")) {
                    getMessagingEngine().pollLocal(parts[1], parts[2]);
                } else if (type.equals("ACK")) {
                    getMessagingEngine().acknowledgeMessageLocal(parts[1]);
                } else if (type.equals("NACK")) {
                    getMessagingEngine().nackMessageLocal(parts[1], Boolean.parseBoolean(parts[2]));
                } else if (type.equals("CREATE_EXCHANGE")) {
                    getQueueService().createExchangeLocal(parts[1], ExchangeType.valueOf(parts[2]), Boolean.parseBoolean(parts[3]));
                } else if (type.equals("DELETE_EXCHANGE")) {
                    getQueueService().deleteExchangeLocal(parts[1]);
                } else if (type.equals("CREATE_QUEUE")) {
                    getQueueService().createQueueLocal(parts[1], parts[2], Boolean.parseBoolean(parts[3]), Boolean.parseBoolean(parts[4]));
                } else if (type.equals("DELETE_QUEUE")) {
                    getQueueService().deleteQueueLocal(parts[1]);
                } else if (type.equals("BIND")) {
                    getQueueService().bindLocal(parts[1], parts[2], decodeValue(parts[3]));
                } else if (type.equals("UNBIND")) {
                    getQueueService().unbindLocal(parts[1], parts[2], decodeValue(parts[3]));
                }

                // Persist the last applied index/term for crash recovery
                QuarkusTransaction.requiringNew().run(() -> {
                    RaftMetadata metadata = RaftMetadata.findById(getNodeId());
                    if (metadata == null) {
                        metadata = new RaftMetadata(getNodeId(), term, null, index, term);
                        metadata.persist();
                    } else {
                        metadata.lastAppliedIndex = index;
                        metadata.lastAppliedTerm = term;
                        if (metadata.currentTerm < term) {
                            metadata.currentTerm = term;
                        }
                    }
                });
            } catch (Exception e) {
                LOG.error("Failed to apply transaction: " + command, e);
                return org.apache.ratis.protocol.Message.valueOf("FAILURE|" + e.getMessage());
            }

            return org.apache.ratis.protocol.Message.valueOf("SUCCESS");
        }, SM_EXECUTOR);
    }

    /**
     * Decodes a Base64-encoded value back to a plain string.
     * Returns {@code null} for null or "null" inputs. Falls back to returning
     * the raw value if Base64 decoding fails (for backward compatibility with
     * older non-encoded messages).
     *
     * @param value the Base64-encoded string (or "null")
     * @return the decoded string, or {@code null}
     */
    private String decodeValue(String value) {
        if (value == null || value.equals("null")) {
            return null;
        }
        try {
            return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            // Fallback for old non-base64 messages
            return value;
        }
    }
}
