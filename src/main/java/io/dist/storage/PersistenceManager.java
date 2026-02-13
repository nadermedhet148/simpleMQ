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

    void onStart(@Observes StartupEvent ev) {
        LOG.info("Broker starting, recovering messages from database...");
        recoverMessages();
    }

    @Transactional
    public void recoverMessages() {
        try {
            List<Message> messages = Message.listAll();
            LOG.info("Found " + messages.size() + " messages in database for recovery.");
            for (Message message : messages) {
                storageService.getBuffer(message.queueName).enqueue(message);
            }
        } catch (Exception e) {
            LOG.error("Failed to recover messages from database", e);
        }
    }

    @Transactional
    public void saveMessage(Message message) {
        message.persist();
    }

    @Transactional
    public void removeMessage(String messageId) {
        Message.deleteById(messageId);
    }
}
