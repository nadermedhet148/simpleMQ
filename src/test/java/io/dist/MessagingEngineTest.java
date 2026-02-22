package io.dist;

import io.dist.model.ExchangeType;
import io.dist.model.Message;
import io.dist.model.MessageStatus;
import io.dist.service.MessagingEngine;
import io.dist.service.QueueService;
import io.quarkus.test.TestTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class MessagingEngineTest {

    @Inject
    MessagingEngine messagingEngine;

    @Inject
    QueueService queueService;

    @Test
    @TestTransaction
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
        Message m1 = messagingEngine.poll("q1");
        assertNotNull(m1);
        assertEquals("hello q1", m1.payload);
        assertEquals(MessageStatus.DELIVERED, m1.status);
        assertEquals(1, m1.deliveryCount);

        assertNull(messagingEngine.poll("q2"));

        // 3. Acknowledge m1
        messagingEngine.acknowledgeMessage(m1.id);
        Message m1_acked = Message.findById(m1.id);
        assertEquals(MessageStatus.ACKED, m1_acked.status);
    }

    @Test
    @TestTransaction
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
        Message m1 = messagingEngine.poll("fq1");
        assertNotNull(m1);
        assertEquals("broadcast", m1.payload);

        Message m2 = messagingEngine.poll("fq2");
        assertNotNull(m2);
        assertEquals("broadcast", m2.payload);
    }

    @Test
    @TestTransaction
    void testNackWithRequeue() {
        queueService.createExchange("ex-nack", ExchangeType.DIRECT, true);
        queueService.createQueue("qn", "group1", true, false);
        queueService.bind("ex-nack", "qn", "k");

        messagingEngine.publish("ex-nack", "k", "nack-me");
        Message m = messagingEngine.poll("qn");
        assertNotNull(m);
        assertEquals(1, m.deliveryCount);

        // Nack with requeue
        messagingEngine.nackMessage(m.id, true);
        
        // Should be back in memory and status PENDING in DB
        Message m_requeued = Message.findById(m.id);
        assertEquals(MessageStatus.PENDING, m_requeued.status);

        Message m2 = messagingEngine.poll("qn");
        assertNotNull(m2);
        assertEquals(m.id, m2.id);
        assertEquals(2, m2.deliveryCount);
    }

    @Test
    @TestTransaction
    void testDLQWorkflow() {
        queueService.createExchange("ex-dlq", ExchangeType.DIRECT, true);
        queueService.createQueue("qd", "group1", true, false);
        queueService.bind("ex-dlq", "qd", "k");

        messagingEngine.publish("ex-dlq", "k", "dlq-me");

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
        Message m_in_db = Message.findById(lastMsgId);
        assertEquals(MessageStatus.DLQ, m_in_db.status);
        assertEquals("DLQ.qd", m_in_db.queueName);

        // Check DLQ
        Message m_dlq = messagingEngine.poll("DLQ.qd");
        assertNotNull(m_dlq);
        assertEquals(MessageStatus.DELIVERED, m_dlq.status); // poll marks it delivered
    }
}
