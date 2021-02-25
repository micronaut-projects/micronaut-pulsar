package example.listeners;

import io.micronaut.core.convert.value.ConvertibleValues;
import example.dto.PulsarMessage;
import io.micronaut.pulsar.annotation.PulsarConsumer;
import io.micronaut.pulsar.annotation.PulsarSubscription;
import io.micronaut.websocket.WebSocketBroadcaster;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PulsarSubscription(subscriptionName = "pulsar-test-subscription", subscriptionType = SubscriptionType.Shared)
public class PulsarSubscriber {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarSubscriber.class);
    private final WebSocketBroadcaster broadcaster;

    public PulsarSubscriber(WebSocketBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @PulsarConsumer(topic = "persistent://public/default/messages", consumerName = "shared-consumer-tester")
    public void messagePrinter(PulsarMessage message) {
        LOGGER.info("A message was received {}. Sent on {}", message.getMessage(), message.getSent());
        broadcaster.broadcastAsync(message, t -> isPropperChannel(t.getUriVariables()));
    }

    private boolean isPropperChannel(ConvertibleValues<?> uriVariables) {
        if (!"public".equalsIgnoreCase(uriVariables.get("tenant", String.class, null))) {
            return false;
        }
        if (!"default".equalsIgnoreCase(uriVariables.get("namespace", String.class, null))) {
            return false;
        }
        return "messages".equalsIgnoreCase(uriVariables.get("tenant", String.class, null));
    }
}
