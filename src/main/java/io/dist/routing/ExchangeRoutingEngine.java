package io.dist.routing;

import io.dist.model.Binding;
import io.dist.model.Exchange;
import io.dist.model.ExchangeType;
import io.dist.model.Message;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Core routing engine that resolves which queues a published message should
 * be delivered to.
 *
 * <p>When a message is published to an exchange this engine:</p>
 * <ol>
 *   <li>Looks up the {@link Exchange} entity by name.</li>
 *   <li>Fetches all {@link Binding}s associated with that exchange.</li>
 *   <li>Selects the appropriate {@link RoutingStrategy} based on the
 *       exchange type (DIRECT or FANOUT).</li>
 *   <li>Filters bindings through the strategy and returns the list of
 *       matching queue names.</li>
 * </ol>
 *
 * @see DirectRoutingStrategy
 * @see FanoutRoutingStrategy
 * @see io.dist.service.MessagingEngine#publish(String, String, String)
 */
@ApplicationScoped
public class ExchangeRoutingEngine {

    /** Singleton strategy instance for DIRECT exchange routing. */
    private final DirectRoutingStrategy directStrategy = new DirectRoutingStrategy();

    /** Singleton strategy instance for FANOUT exchange routing. */
    private final FanoutRoutingStrategy fanoutStrategy = new FanoutRoutingStrategy();

    /**
     * Determines the target queue names for a given message based on its
     * exchange and routing key.
     *
     * @param message the message to route (must have {@code exchange} set)
     * @return list of queue names that should receive the message; empty if
     *         the exchange does not exist or no bindings match
     */
    public List<String> getTargetQueues(Message message) {
        Exchange exchange = Exchange.findById(message.exchange);
        if (exchange == null) {
            return List.of();
        }

        // Fetch all bindings for this exchange from the database
        List<Binding> bindings = Binding.find("exchangeName", message.exchange).list();

        // Select the routing strategy based on exchange type
        RoutingStrategy strategy = getStrategy(exchange.type);

        // Filter bindings and collect matching queue names
        return bindings.stream()
                .filter(binding -> strategy.matches(message, binding))
                .map(binding -> binding.queueName)
                .collect(Collectors.toList());
    }

    /**
     * Returns the routing strategy implementation for the given exchange type.
     *
     * @param type the exchange type
     * @return the corresponding routing strategy
     */
    private RoutingStrategy getStrategy(ExchangeType type) {
        return switch (type) {
            case DIRECT -> directStrategy;
            case FANOUT -> fanoutStrategy;
        };
    }
}
