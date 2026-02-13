package io.dist.routing;

import io.dist.model.Binding;
import io.dist.model.Message;

public class DirectRoutingStrategy implements RoutingStrategy {
    @Override
    public boolean matches(Message message, Binding binding) {
        if (message.routingKey == null) {
            return binding.routingKey == null;
        }
        return message.routingKey.equals(binding.routingKey);
    }
}
