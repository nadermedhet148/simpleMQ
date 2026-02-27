package io.dist.cluster;

import io.dist.model.Message;
import io.dist.model.MessageStatus;
import io.dist.storage.PersistenceManager;
import io.dist.storage.StorageService;
import jakarta.enterprise.inject.spi.CDI;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

public class SimpleStateMachine extends BaseStateMachine {
    private static final Logger LOG = Logger.getLogger(SimpleStateMachine.class);

    private PersistenceManager getPersistenceManager() {
        return CDI.current().select(PersistenceManager.class).get();
    }

    private StorageService getStorageService() {
        return CDI.current().select(StorageService.class).get();
    }

    @Override
    public CompletableFuture<org.apache.ratis.protocol.Message> applyTransaction(TransactionContext trx) {
        final String command = trx.getLogEntry().getStateMachineLogEntry().getLogData().toString(StandardCharsets.UTF_8);
        LOG.debugf("Applying transaction: %s", command);

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

                // Persist to local SQLite and Enqueue to local buffer
                getPersistenceManager().saveMessage(msg);
                getStorageService().getBuffer(msg.queueName).enqueue(msg);
                
                LOG.infof("Replicated and persisted message %s to queue %s", msg.id, msg.queueName);
            }
        } catch (Exception e) {
            LOG.error("Failed to apply transaction", e);
            return CompletableFuture.completedFuture(org.apache.ratis.protocol.Message.valueOf("FAILURE"));
        }

        return CompletableFuture.completedFuture(org.apache.ratis.protocol.Message.valueOf("SUCCESS"));
    }
}
