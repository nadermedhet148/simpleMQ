package io.dist.cluster;

import io.dist.model.Message;
import io.dist.model.MessageStatus;
import io.dist.storage.PersistenceManager;
import io.dist.storage.StorageService;
import io.dist.model.RaftMetadata;
import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.enterprise.inject.spi.CDI;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class SimpleStateMachine extends BaseStateMachine {
    private static final Logger LOG = Logger.getLogger(SimpleStateMachine.class);
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

    @Override
    public CompletableFuture<org.apache.ratis.protocol.Message> applyTransaction(TransactionContext trx) {
        final long index = trx.getLogEntry().getIndex();
        final long term = trx.getLogEntry().getTerm();
        final String command = trx.getLogEntry().getStateMachineLogEntry().getLogData().toString(StandardCharsets.UTF_8);
        LOG.infof("Applying transaction: %s at index %d", command, index);

        try {
            // Simple command format: "PUBLISH|id|payload|routingKey|exchange|queueName|timestamp"
            String[] parts = command.split("\\|");
            if (parts[0].equals("PUBLISH")) {
                Message msg = new Message();
                msg.id = parts[1];
                msg.payload = parts[2];
                msg.routingKey = parts[3];
                msg.exchange = parts[4];
                msg.queueName = parts[5];
                msg.timestamp = LocalDateTime.parse(parts[6]);
                msg.deliveryCount = 0;
                msg.status = MessageStatus.PENDING;

                LOG.infof("Processing message %s for queue %s", msg.id, msg.queueName);

                // Persist to local SQLite and Enqueue to local buffer
                PersistenceManager pm = getPersistenceManager();
                pm.saveMessage(msg);

                StorageService ss = getStorageService();
                ss.getBuffer(msg.queueName).enqueue(msg);
                
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

                LOG.infof("Successfully replicated and persisted message %s to queue %s", msg.id, msg.queueName);
            }
        } catch (Exception e) {
            LOG.error("Failed to apply transaction: " + command, e);
            return CompletableFuture.completedFuture(org.apache.ratis.protocol.Message.valueOf("FAILURE|" + e.getMessage()));
        }

        return CompletableFuture.completedFuture(org.apache.ratis.protocol.Message.valueOf("SUCCESS"));
    }
}
