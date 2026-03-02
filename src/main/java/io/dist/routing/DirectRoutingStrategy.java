package io.dist.routing;

import io.dist.model.Binding;
import io.dist.model.Message;

/**
 * Routing strategy for {@link io.dist.model.ExchangeType#DIRECT} exchanges.
 *
 * <p>A direct exchange routes a message to a queue only when the message's
 * routing key exactly matches the binding's routing key. If both keys are
 * {@code null} the match also succeeds (null == null).</p>
 *
 * @see FanoutRoutingStrategy
 * @see ExchangeRoutingEngine
 */
public class DirectRoutingStrategy implements RoutingStrategy {

    /**
     * Returns {@code true} when the message's routing key equals the
     * binding's routing key (exact string match, null-safe).
     */
    @Override
    public boolean matches(Message message, Binding binding) {
        if (message.routingKey == null) {
            return binding.routingKey == null;
        }
        return message.routingKey.equals(binding.routingKey);
    }
}
