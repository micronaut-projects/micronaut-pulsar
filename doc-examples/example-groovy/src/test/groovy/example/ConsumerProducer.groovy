package example;

import io.micronaut.pulsar.annotation.PulsarConsumer;
import io.micronaut.pulsar.annotation.PulsarProducer;
import io.micronaut.pulsar.annotation.PulsarSubscription;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@PulsarSubscription(subscriptionName = "pulsar-jtest-subscription", subscriptionType = SubscriptionType.Shared) // <1>
class ConsumerProducer { // <2>
    @PulsarConsumer(topic = "persistent://public/default/messages-kotlin-docs", consumerName = "shared-consumer-ktester") // <3>
    void messagePrinter(String message) { // <4>
        try {
            String changed = report(message).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        //...
    }


    @PulsarProducer(topic = "persistent://public/default/reports-kotlin-docs", producerName = "report-producer-kotlin") // <5>
    CompletableFuture<String> report(String message) { // <6>
        return CompletableFuture.supplyAsync(() -> String.format("Reporting message %s", message)); // <7>
    }
}