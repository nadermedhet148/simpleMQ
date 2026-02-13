package io.dist.routing;

import io.dist.model.Binding;
import io.dist.model.Message;

public class FanoutRoutingStrategy implements RoutingStrategy {
    @Override
    public boolean matches(Message message, Binding binding) {
        return true; // Fanout matches all bindings to the exchange
    }
}
