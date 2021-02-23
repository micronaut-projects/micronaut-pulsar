package io.micronaut.pulsar.example.listeners;

import io.micronaut.pulsar.annotation.PulsarConsumer;
import io.micronaut.pulsar.annotation.PulsarSubscription;

@PulsarSubscription(subscriptionName = "reports")
public class ReportsTracker {

    @PulsarConsumer(consumerName = "report-listener", topic = "private/reports/messages")
    public void report(String message) {

    }
}
