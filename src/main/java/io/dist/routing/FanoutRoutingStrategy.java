package io.dist.routing;

import io.dist.model.Binding;
import io.dist.model.Message;

/**
 * Routing strategy for {@link io.dist.model.ExchangeType#FANOUT} exchanges.
 *
 * <p>A fanout exchange broadcasts every message to <b>all</b> queues that are
 * bound to it, regardless of the message's routing key. This implements the
 * classic publish-subscribe (pub-sub) pattern.</p>
 *
 * @see DirectRoutingStrategy
 * @see ExchangeRoutingEngine
 */
public class FanoutRoutingStrategy implements RoutingStrategy {

    /**
     * Always returns {@code true} because fanout exchanges deliver to every
     * bound queue without filtering.
     */
    @Override
    public boolean matches(Message message, Binding binding) {
        return true;
    }
}
