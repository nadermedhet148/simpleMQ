package io.dist.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;

/**
 * Persistent entity representing a binding between an {@link Exchange} and a {@link Queue}.
 *
 * <p>A binding tells the routing engine which queues should receive messages
 * published to a given exchange. For {@link ExchangeType#DIRECT} exchanges
 * the {@link #routingKey} is used to filter messages; for
 * {@link ExchangeType#FANOUT} exchanges the routing key is ignored and all
 * bound queues receive every message.</p>
 *
 * @see Exchange
 * @see Queue
 * @see io.dist.routing.ExchangeRoutingEngine
 */
@Entity
@Table(name = "bindings")
public class Binding extends PanacheEntityBase {

    /** Auto-generated primary key for this binding record. */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    /** Name of the exchange this binding is attached to. */
    public String exchangeName;

    /** Name of the queue that will receive routed messages. */
    public String queueName;

    /**
     * Routing key used for DIRECT exchange matching.
     * May be {@code null} for FANOUT exchanges where all bindings match.
     */
    public String routingKey;

    /** Default no-arg constructor required by JPA / Hibernate. */
    public Binding() {}

    /**
     * Creates a new binding linking an exchange to a queue with an optional routing key.
     *
     * @param exchangeName the exchange name
     * @param queueName    the queue name
     * @param routingKey   the routing key (may be {@code null} for fanout bindings)
     */
    public Binding(String exchangeName, String queueName, String routingKey) {
        this.exchangeName = exchangeName;
        this.queueName = queueName;
        this.routingKey = routingKey;
    }
}
