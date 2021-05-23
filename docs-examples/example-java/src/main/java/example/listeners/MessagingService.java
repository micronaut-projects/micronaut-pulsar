/*
 * Copyright 2017-2021 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package example.listeners;

import example.dto.PulsarMessage;
import io.micronaut.context.annotation.Context;
import io.micronaut.core.convert.value.ConvertibleValues;
import io.micronaut.http.MediaType;
import io.micronaut.pulsar.annotation.PulsarConsumer;
import io.micronaut.pulsar.annotation.PulsarProducer;
import io.micronaut.pulsar.annotation.PulsarSubscription;
import io.micronaut.websocket.WebSocketBroadcaster;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Scope;

/**
 * Pulsar consumers.
 *
 * @author Haris Secic
 * @since 1.0
 */
@PulsarSubscription(subscriptionName = "pulsar-test-subscription", subscriptionType = SubscriptionType.Shared)
public class MessagingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessagingService.class);
    private final WebSocketBroadcaster broadcaster;

    public MessagingService(WebSocketBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    /**
     * @param message data received on pulsar topic
     */
    @PulsarConsumer(topic = "persistent://public/default/messages", consumerName = "shared-consumer-tester")
    public void messagePrinter(PulsarMessage message) {
        LOGGER.info("A message was received {}. Sent on {}", message.getMessage(), message.getSent());
        broadcaster.broadcastAsync(message, MediaType.APPLICATION_JSON_TYPE, t -> isProperChannel(t.getUriVariables()));
        report(message.toMessage());
    }

    /**
     * @param message text to send to reports topic
     * @return message string value
     */
    // when inside other beans no @PulsarProducerClient is required on the class
    @PulsarProducer(topic = "persistent://private/reports/messages", producerName = "report-producer")
    public String report(String message) {
        // should happen before message is being sent to Pulsar by default
        LOGGER.info("Sending message \"{}\"", message);
        return message;
    }

    private boolean isProperChannel(ConvertibleValues<?> uriVariables) {
        if (!"public".equalsIgnoreCase(uriVariables.get("tenant", String.class, null))) {
            return false;
        }
        if (!"default".equalsIgnoreCase(uriVariables.get("namespace", String.class, null))) {
            return false;
        }
        return "messages".equalsIgnoreCase(uriVariables.get("topic", String.class, null));
    }
}
