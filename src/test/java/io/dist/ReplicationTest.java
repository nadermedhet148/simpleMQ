package io.dist;

import io.dist.cluster.RaftService;
import io.dist.service.MessagingEngine;
import io.dist.storage.StorageService;
import io.dist.service.QueueService;
import io.dist.model.ExchangeType;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class ReplicationTest {

    @Inject
    RaftService raftService;

    @Inject
    MessagingEngine messagingEngine;

    @Inject
    StorageService storageService;

    @Inject
    QueueService queueService;

    @BeforeEach
    void setup() {
        // Ensure the single node is leader
        Awaitility.await().atMost(Duration.ofSeconds(15))
                .until(() -> raftService.isLeader());
        
        // Setup a queue and exchange
        if (io.dist.model.Exchange.findById("test-exchange") == null) {
            queueService.createExchange("test-exchange", ExchangeType.DIRECT, true).await().indefinitely();
        }
        if (io.dist.model.Queue.findById("test-queue") == null) {
            queueService.createQueue("test-queue", "default", true, false).await().indefinitely();
        }
        queueService.bind("test-exchange", "test-queue", "routing.key").await().indefinitely();
    }

    @Test
    void testMessageReplication() {
        String payload = "Replicated Message Content";
        
        // This should go through Raft -> StateMachine -> Storage
        messagingEngine.publish("test-exchange", "routing.key", payload).await().indefinitely();
        
        // Check if it reached the buffer via StateMachine
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> storageService.getBuffer("test-queue").size() > 0);
        
        var msg = messagingEngine.poll("test-queue").await().indefinitely();
        assertNotNull(msg);
        assertTrue(msg.payload.contains(payload));
    }
}
