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

public class SimpleStateMachine extends BaseStateMachine {
    private static final Logger LOG = Logger.getLogger(SimpleStateMachine.class);

    // Single-threaded executor so all state-machine DB writes are serialized.
    // Multiple applyTransaction() calls running concurrently on the worker pool
    // cause SQLite "database is locked" errors because SQLite only allows one
    // write transaction at a time.
    private static final java.util.concurrent.ExecutorService SM_EXECUTOR =
        java.util.concurrent.Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "raft-sm-executor");
            t.setDaemon(true);
            return t;
        });

    private String nodeId;

    public SimpleStateMachine() {
        // We will initialize nodeId from RaftService if possible, or just use a default
        // But in Quarkus, we can't easily inject into StateMachine if it's created by new
        // So we will get it from Config or RaftService via CDI
    }

    private String getNodeId() {
        if (nodeId == null) {
            nodeId = CDI.current().select(RaftService.class).get().getNodeId();
        }
        return nodeId;
    }

    private PersistenceManager getPersistenceManager() {
        return CDI.current().select(PersistenceManager.class).get();
    }

    private StorageService getStorageService() {
        return CDI.current().select(StorageService.class).get();
    }

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

    private QueueService getQueueService() {
        return CDI.current().select(QueueService.class).get();
    }

    private MessagingEngine getMessagingEngine() {
        return CDI.current().select(MessagingEngine.class).get();
    }

    private MetricsService getMetricsService() {
        return CDI.current().select(MetricsService.class).get();
    }

    @Override
    public CompletableFuture<org.apache.ratis.protocol.Message> applyTransaction(TransactionContext trx) {
        return CompletableFuture.supplyAsync(() -> {
            final long index = trx.getLogEntry().getIndex();
            final long term = trx.getLogEntry().getTerm();
            final String command = trx.getLogEntry().getStateMachineLogEntry().getLogData().toString(StandardCharsets.UTF_8);
            LOG.infof("Applying transaction: %s at index %d (Thread: %s)", command, index, Thread.currentThread().getName());

            try {
                String[] parts = command.split("\\|", -1);
                String type = parts[0];
                
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

                // Update last applied index in RaftMetadata
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
