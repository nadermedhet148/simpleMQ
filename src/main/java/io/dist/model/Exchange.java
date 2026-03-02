package io.dist.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;

/**
 * Persistent entity representing a message exchange in simpleMQ.
 *
 * <p>An exchange receives messages from producers and routes them to one or
 * more {@link Queue}s based on its {@link ExchangeType} and the
 * {@link Binding}s that link it to queues.</p>
 *
 * <ul>
 *   <li><b>DIRECT</b> – routes to queues whose binding routing key matches
 *       the message's routing key exactly.</li>
 *   <li><b>FANOUT</b> – broadcasts to all bound queues regardless of
 *       routing key.</li>
 * </ul>
 *
 * @see ExchangeType
 * @see Binding
 * @see io.dist.routing.ExchangeRoutingEngine
 */
@Entity
@Table(name = "exchanges")
public class Exchange extends PanacheEntityBase {

    /** Unique name identifying this exchange. Also serves as the primary key. */
    @Id
    public String name;

    /** The routing strategy type (DIRECT or FANOUT). */
    public ExchangeType type;

    /** Whether this exchange survives broker restarts (persisted to SQLite). */
    public boolean durable;

    /** Default no-arg constructor required by JPA / Hibernate. */
    public Exchange() {}

    /**
     * Creates a new exchange with the given properties.
     *
     * @param name    unique exchange name
     * @param type    routing strategy type
     * @param durable whether the exchange is persisted to disk
     */
    public Exchange(String name, ExchangeType type, boolean durable) {
        this.name = name;
        this.type = type;
        this.durable = durable;
    }
}
