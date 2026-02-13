package io.dist.routing;

import io.dist.model.Binding;
import io.dist.model.Message;
import java.util.List;

public interface RoutingStrategy {
    boolean matches(Message message, Binding binding);
}
