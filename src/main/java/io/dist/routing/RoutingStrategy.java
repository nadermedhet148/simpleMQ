package io.dist.routing;

import io.dist.model.Binding;
import io.dist.model.Message;
import java.util.List;

/**
 * Strategy interface for determining whether a message matches a given binding.
 *
 * <p>Implementations define the routing logic for different exchange types.
 * The {@link ExchangeRoutingEngine} selects the appropriate strategy based on
 * the exchange type and applies it to each binding to determine which queues
 * should receive a published message.</p>
 *
 * @see DirectRoutingStrategy
 * @see FanoutRoutingStrategy
 * @see ExchangeRoutingEngine
 */
public interface RoutingStrategy {

    /**
     * Determines whether the given message should be routed to the queue
     * specified by the binding.
     *
     * @param message the message being routed
     * @param binding the binding to evaluate against
     * @return {@code true} if the message matches the binding and should be
     *         delivered to the binding's queue
     */
    boolean matches(Message message, Binding binding);
}
