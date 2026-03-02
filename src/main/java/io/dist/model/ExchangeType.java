package io.dist.model;

/**
 * Defines the supported exchange types in simpleMQ.
 *
 * <p>An exchange type determines how messages are routed from an exchange
 * to the queues that are bound to it.</p>
 *
 * @see Exchange
 * @see io.dist.routing.RoutingStrategy
 */
public enum ExchangeType {

    /**
     * Direct exchange: routes a message to queues whose binding routing key
     * exactly matches the message's routing key.
     */
    DIRECT,

    /**
     * Fanout exchange: broadcasts a message to <b>all</b> queues bound to
     * the exchange, regardless of routing key.
     */
    FANOUT
}
