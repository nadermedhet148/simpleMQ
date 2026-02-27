package io.dist;

import io.dist.model.ExchangeType;
import io.dist.model.Message;
import io.dist.model.MessageStatus;
import io.dist.service.MessagingEngine;
import io.dist.service.QueueService;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class MessagingEngineTest {

    @Inject
    MessagingEngine messagingEngine;

    @Inject
    QueueService queueService;

    @Inject
    io.dist.storage.StorageService storageService;

    @BeforeEach
    @Transactional
    void setup() {
        storageService.clear();
        io.dist.model.Message.deleteAll();
        io.dist.model.Exchange.deleteAll();
        io.dist.model.Queue.deleteAll();
        io.dist.model.Binding.deleteAll();
    }

    @Transactional
    Message getMessage(String id) {
        return Message.findById(id);
    }

    @Test
    void testDirectRoutingAndFlow() {
        // Setup: Exchange, Queues and Bindings
        queueService.createExchange("direct-ex", ExchangeType.DIRECT, true);
        queueService.createQueue("q1", "group1", true, false);
        queueService.createQueue("q2", "group1", true, false);
        queueService.bind("direct-ex", "q1", "key1");
        queueService.bind("direct-ex", "q2", "key2");

        // 1. Publish to key1
        messagingEngine.publish("direct-ex", "key1", "hello q1");

        // 2. Verify it's in q1 and not in q2
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> storageService.getBuffer("q1").size() > 0);
        
        Message m1 = messagingEngine.poll("q1");
        assertNotNull(m1);
        assertEquals("hello q1", m1.payload);
        assertEquals(MessageStatus.DELIVERED, m1.status);

        assertNull(messagingEngine.poll("q2"));

        // 3. Acknowledge m1
        messagingEngine.acknowledgeMessage(m1.id);
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> getMessage(m1.id).status == MessageStatus.ACKED);
    }

    @Test
    void testFanoutRouting() {
        // Setup: Fanout Exchange
        queueService.createExchange("fanout-ex", ExchangeType.FANOUT, true);
        queueService.createQueue("fq1", "group1", true, false);
        queueService.createQueue("fq2", "group1", true, false);
        queueService.bind("fanout-ex", "fq1", null);
        queueService.bind("fanout-ex", "fq2", null);

        // 1. Publish
        messagingEngine.publish("fanout-ex", "any-key", "broadcast");

        // 2. Verify both queues got it
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> storageService.getBuffer("fq1").size() > 0 && storageService.getBuffer("fq2").size() > 0);
        
        Message m1 = messagingEngine.poll("fq1");
        assertNotNull(m1);
        assertEquals("broadcast", m1.payload);

        Message m2 = messagingEngine.poll("fq2");
        assertNotNull(m2);
        assertEquals("broadcast", m2.payload);
    }

    @Test
    void testNackWithRequeue() {
        queueService.createExchange("ex-nack", ExchangeType.DIRECT, true);
        queueService.createQueue("qn", "group1", true, false);
        queueService.bind("ex-nack", "qn", "k");

        messagingEngine.publish("ex-nack", "k", "nack-me");
        
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> storageService.getBuffer("qn").size() > 0);
        
        Message m = messagingEngine.poll("qn");
        assertNotNull(m);
        assertEquals(1, m.deliveryCount);

        // Nack with requeue
        messagingEngine.nackMessage(m.id, true);
        
        // Should be back in memory and status PENDING in DB
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> getMessage(m.id).status == MessageStatus.PENDING);

        Message m2 = messagingEngine.poll("qn");
        assertNotNull(m2);
        assertEquals(m.id, m2.id);
        assertEquals(2, m2.deliveryCount);
    }

    @Test
    void testDLQWorkflow() {
        queueService.createExchange("ex-dlq", ExchangeType.DIRECT, true);
        queueService.createQueue("qd", "group1", true, false);
        queueService.bind("ex-dlq", "qd", "k");

        messagingEngine.publish("ex-dlq", "k", "dlq-me");
        
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> storageService.getBuffer("qd").size() > 0);

        // Simulate 3 failed attempts
        String lastMsgId = null;
        for (int i = 0; i < 3; i++) {
            Message m = messagingEngine.poll("qd");
            assertNotNull(m);
            lastMsgId = m.id;
            messagingEngine.nackMessage(m.id, true);
        }

        // 4th poll should find nothing in qd (it went to DLQ after 3rd nack)
        assertNull(messagingEngine.poll("qd"));

        // Verify status in DB is DLQ before polling it from DLQ
        final String lastId = lastMsgId;
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> {
                    Message m = getMessage(lastId);
                    return m != null && m.status == MessageStatus.DLQ;
                });
        
        Message m_in_db = getMessage(lastId);
        assertEquals(MessageStatus.DLQ, m_in_db.status);
        assertEquals("DLQ.qd", m_in_db.queueName);

        // Check DLQ
        Message m_dlq = messagingEngine.poll("DLQ.qd");
        assertNotNull(m_dlq);
        assertEquals(MessageStatus.DELIVERED, m_dlq.status); // poll marks it delivered
    }
}
