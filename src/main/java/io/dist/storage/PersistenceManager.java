package io.dist.storage;

import io.dist.model.Message;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.util.List;
import org.jboss.logging.Logger;

@ApplicationScoped
public class PersistenceManager {
    private static final Logger LOG = Logger.getLogger(PersistenceManager.class);

    @Inject
    StorageService storageService;

    @org.eclipse.microprofile.config.inject.ConfigProperty(name = "simplemq.persistence.enabled", defaultValue = "true")
    boolean persistenceEnabled;

    void onStart(@Observes StartupEvent ev) {
        if (persistenceEnabled) {
            LOG.info("Broker starting, recovering messages from database...");
            recoverMessages();
        } else {
            LOG.info("Broker starting in ephemeral mode, skipping recovery.");
        }
    }

    public boolean isPersistenceEnabled() {
        return persistenceEnabled;
    }

    @Transactional
    public void recoverMessages() {
        try {
            List<Message> messages = Message.list("status != ?1 and status != ?2", io.dist.model.MessageStatus.ACKED, io.dist.model.MessageStatus.DLQ);
            LOG.info("Found " + messages.size() + " messages in database for recovery.");
            for (Message message : messages) {
                if (message.status == io.dist.model.MessageStatus.DELIVERED) {
                    message.status = io.dist.model.MessageStatus.PENDING;
                }
                storageService.getBuffer(message.queueName).enqueue(message);
            }
        } catch (Exception e) {
            LOG.error("Failed to recover messages from database", e);
        }
    }

    @Transactional
    public void saveMessage(Message message) {
        if (Message.findById(message.id) == null) {
            message.persist();
        } else {
            LOG.infof("Message %s already exists in database, skipping persist", message.id);
        }
    }

    @Transactional
    public void removeMessage(String messageId) {
        Message.deleteById(messageId);
    }
}
