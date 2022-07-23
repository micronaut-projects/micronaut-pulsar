package example;

import io.micronaut.pulsar.annotation.PulsarConsumer;
import io.micronaut.pulsar.annotation.PulsarProducer;
import io.micronaut.pulsar.annotation.PulsarSubscription;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@PulsarSubscription(subscriptionName = "pulsar-jtest-subscription", subscriptionType = SubscriptionType.Shared) // <1>
public class ConsumerProducer { // <2>
    @PulsarConsumer(topic = "persistent://public/default/messages-java-docs", consumerName = "shared-consumer-jtester") // <3>
    public void messagePrinter(String message) { // <4>
        try {
            String changed = report(message).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        //...
    }


    @PulsarProducer(topic = "persistent://public/default/reports-java-docs", producerName = "report-producer-java") // <5>
    public CompletableFuture<String> report(String message) { // <6>
        return CompletableFuture.supplyAsync(() -> String.format("Reporting message %s", message)); // <7>
    }
}