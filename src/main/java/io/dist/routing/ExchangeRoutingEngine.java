package io.dist.routing;

import io.dist.model.Binding;
import io.dist.model.Exchange;
import io.dist.model.ExchangeType;
import io.dist.model.Message;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.stream.Collectors;

@ApplicationScoped
public class ExchangeRoutingEngine {

    private final DirectRoutingStrategy directStrategy = new DirectRoutingStrategy();
    private final FanoutRoutingStrategy fanoutStrategy = new FanoutRoutingStrategy();

    public List<String> getTargetQueues(Message message) {
        Exchange exchange = Exchange.findById(message.exchange);
        if (exchange == null) {
            return List.of();
        }

        List<Binding> bindings = Binding.find("exchangeName", message.exchange).list();
        
        RoutingStrategy strategy = getStrategy(exchange.type);
        
        return bindings.stream()
                .filter(binding -> strategy.matches(message, binding))
                .map(binding -> binding.queueName)
                .collect(Collectors.toList());
    }

    private RoutingStrategy getStrategy(ExchangeType type) {
        return switch (type) {
            case DIRECT -> directStrategy;
            case FANOUT -> fanoutStrategy;
        };
    }
}
