package io.dist;

import io.dist.model.Message;
import io.dist.storage.InMemoryBuffer;
import io.dist.storage.PersistenceManager;
import io.dist.storage.StorageService;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class PersistenceTest {

    @Inject
    PersistenceManager persistenceManager;

    @Inject
    StorageService storageService;

    @Test
    @TestTransaction
    void testSaveAndRecover() {
        // 1. Save a message
        Message msg = new Message("payload1", "key1", "ex1", "q1");
        persistenceManager.saveMessage(msg);

        // 2. Verify it is in the database
        Message found = Message.findById(msg.id);
        assertNotNull(found);
        assertEquals("payload1", found.payload);

        // 3. Clear in-memory buffer to simulate recovery
        storageService.getBuffer("q1").dequeue(); // make sure it's empty if it was somehow filled
        assertTrue(storageService.getBuffer("q1").isEmpty());

        // 4. Run recovery
        persistenceManager.recoverMessages();

        // 5. Check if it's back in memory
        InMemoryBuffer buffer = storageService.getBuffer("q1");
        assertFalse(buffer.isEmpty());
        Message recovered = buffer.dequeue();
        assertEquals("payload1", recovered.payload);
        assertEquals("q1", recovered.queueName);
    }

    @Test
    @TestTransaction
    void testDeleteMessage() {
        Message msg = new Message("payload2", "key2", "ex1", "q2");
        persistenceManager.saveMessage(msg);
        assertNotNull(Message.findById(msg.id));

        persistenceManager.removeMessage(msg.id);
        assertNull(Message.findById(msg.id));
    }
}
