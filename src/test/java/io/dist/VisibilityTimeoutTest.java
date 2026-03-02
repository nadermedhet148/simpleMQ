package io.dist;

import io.dist.model.ExchangeType;
import io.dist.model.Message;
import io.dist.model.MessageStatus;
import io.dist.service.MessagingEngine;
import io.dist.service.QueueService;
import io.dist.storage.StorageService;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class VisibilityTimeoutTest {

    @Inject
    MessagingEngine messagingEngine;

    @Inject
    QueueService queueService;

    @Inject
    StorageService storageService;

    @BeforeEach
    @Transactional
    void setup() {
        storageService.clear();
        Message.deleteAll();
        io.dist.model.Exchange.deleteAll();
        io.dist.model.Queue.deleteAll();
        io.dist.model.Binding.deleteAll();
    }

    @Transactional
    Message getMessage(String id) {
        return Message.findById(id);
    }

    @Test
    void testMessageReappearsAfterVisibilityTimeout() {
        // Setup
        queueService.createExchange("vt-ex", ExchangeType.DIRECT, true).await().indefinitely();
        queueService.createQueue("vt-q", "group1", true, false).await().indefinitely();
        queueService.bind("vt-ex", "vt-q", "vt-key").await().indefinitely();

        // Publish a message
        messagingEngine.publish("vt-ex", "vt-key", "timeout-test").await().indefinitely();

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> storageService.getBuffer("vt-q").size() > 0);

        // Poll the message (consumer receives it but never acks/nacks)
        Message polled = messagingEngine.poll("vt-q").await().indefinitely();
        assertNotNull(polled);
        assertEquals("timeout-test", polled.payload);
        assertEquals(MessageStatus.DELIVERED, polled.status);

        // Queue should be empty now
        assertNull(messagingEngine.poll("vt-q").await().indefinitely());

        // Verify message is tracked in-flight
        assertEquals(1, storageService.getBuffer("vt-q").inFlightSize());

        // Verify deliveredAt is set in DB
        Message dbMsg = getMessage(polled.id);
        assertNotNull(dbMsg.deliveredAt, "deliveredAt should be set after poll");

        // Manually expire the in-flight message with a 0ms timeout to simulate timeout
        storageService.getBuffer("vt-q").expireInFlight(0);

        // The scheduled checker would normally do this, but we trigger redelivery manually
        messagingEngine.checkVisibilityTimeouts();

        // The message should be back in the queue for another consumer
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> storageService.getBuffer("vt-q").size() > 0);

        Message redelivered = messagingEngine.poll("vt-q").await().indefinitely();
        assertNotNull(redelivered, "Message should be redelivered after visibility timeout");
        assertEquals(polled.id, redelivered.id);
        assertEquals("timeout-test", redelivered.payload);
        assertEquals(2, redelivered.deliveryCount);
    }

    @Test
    void testAckedMessageNotRequeued() {
        // Setup
        queueService.createExchange("vt-ex2", ExchangeType.DIRECT, true).await().indefinitely();
        queueService.createQueue("vt-q2", "group1", true, false).await().indefinitely();
        queueService.bind("vt-ex2", "vt-q2", "vt-key2").await().indefinitely();

        messagingEngine.publish("vt-ex2", "vt-key2", "ack-test").await().indefinitely();

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> storageService.getBuffer("vt-q2").size() > 0);

        Message polled = messagingEngine.poll("vt-q2").await().indefinitely();
        assertNotNull(polled);

        // ACK the message
        messagingEngine.acknowledgeMessage(polled.id).await().indefinitely();
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> getMessage(polled.id).status == MessageStatus.ACKED);

        // In-flight should be cleared
        assertEquals(0, storageService.getBuffer("vt-q2").inFlightSize());

        // Trigger timeout check — nothing should be requeued
        messagingEngine.checkVisibilityTimeouts();
        assertNull(messagingEngine.poll("vt-q2").await().indefinitely());
    }

    @Test
    void testExpiredMessageGoesToDLQAfterMaxAttempts() {
        // Setup
        queueService.createExchange("vt-ex3", ExchangeType.DIRECT, true).await().indefinitely();
        queueService.createQueue("vt-q3", "group1", true, false).await().indefinitely();
        queueService.bind("vt-ex3", "vt-q3", "vt-key3").await().indefinitely();

        messagingEngine.publish("vt-ex3", "vt-key3", "dlq-timeout-test").await().indefinitely();

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> storageService.getBuffer("vt-q3").size() > 0);

        // Simulate 3 deliveries that all time out (no ack/nack)
        for (int i = 0; i < 3; i++) {
            Message polled = messagingEngine.poll("vt-q3").await().indefinitely();
            assertNotNull(polled, "Attempt " + (i + 1) + " should return a message");

            // Expire and trigger redelivery
            storageService.getBuffer("vt-q3").expireInFlight(0);
            messagingEngine.checkVisibilityTimeouts();

            if (i < 2) {
                // Should be requeued
                Awaitility.await().atMost(Duration.ofSeconds(5))
                        .until(() -> storageService.getBuffer("vt-q3").size() > 0);
            }
        }

        // After 3 delivery attempts, message should be in DLQ
        assertNull(messagingEngine.poll("vt-q3").await().indefinitely());

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> storageService.getBuffer("DLQ.vt-q3").size() > 0);

        Message dlqMsg = messagingEngine.poll("DLQ.vt-q3").await().indefinitely();
        assertNotNull(dlqMsg, "Message should be in DLQ after max delivery attempts via timeout");
        assertEquals("dlq-timeout-test", dlqMsg.payload);
    }
}
