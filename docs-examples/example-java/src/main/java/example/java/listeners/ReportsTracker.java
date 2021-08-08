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
package example.java.listeners;

import io.micronaut.pulsar.annotation.PulsarConsumer;
import io.micronaut.pulsar.annotation.PulsarSubscription;
import org.apache.pulsar.client.api.SubscriptionType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * Reports topic consumer that outputs data as async flow on request.
 */
@PulsarSubscription(subscriptionName = "reports", subscriptionType = SubscriptionType.Shared)
public class ReportsTracker implements AutoCloseable {

    private final Sinks.Many<String> tracker = Sinks.many().multicast().onBackpressureBuffer();

    /**
     * @param message string to store async
     */
    @PulsarConsumer(consumerName = "report-listener", topic = "persistent://private/reports/messages-java-docs", subscribeAsync = false)
    public void report(String message) {
        tracker.tryEmitNext(message).orThrow();
    }

    /**
     * Consume messages as they come in from pulsar. Creates a "reactive client" that takes in messages as they come in.TO
     *
     * @return flow of message strings
     */
    public Flux<String> subscribe() {
        return tracker.asFlux();
    }

    @Override
    public void close() throws Exception {
        Sinks.EmitResult result = tracker.tryEmitComplete();
        if (result.isFailure()) {
            throw new RuntimeException(String.format("Failed to close the reports stream: %s", result.name()));
        }
    }
}
