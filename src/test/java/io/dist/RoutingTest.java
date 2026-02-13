package io.dist;

import io.dist.model.Binding;
import io.dist.model.Exchange;
import io.dist.model.ExchangeType;
import io.dist.model.Message;
import io.dist.routing.DirectRoutingStrategy;
import io.dist.routing.FanoutRoutingStrategy;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class RoutingTest {

    @Test
    void testDirectRouting() {
        DirectRoutingStrategy strategy = new DirectRoutingStrategy();
        Message msg = new Message("hello", "key1", "ex1", "q1");
        Binding b1 = new Binding("ex1", "q1", "key1");
        Binding b2 = new Binding("ex1", "q2", "key2");

        assertTrue(strategy.matches(msg, b1));
        assertFalse(strategy.matches(msg, b2));
    }

    @Test
    void testFanoutRouting() {
        FanoutRoutingStrategy strategy = new FanoutRoutingStrategy();
        Message msg = new Message("hello", "any-key", "ex1", "q1");
        Binding b1 = new Binding("ex1", "q1", "key1");
        Binding b2 = new Binding("ex1", "q2", "key2");

        assertTrue(strategy.matches(msg, b1));
        assertTrue(strategy.matches(msg, b2));
    }
}
