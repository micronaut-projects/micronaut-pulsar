package example;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@MicronautTest(environments = "pulsar")
class PulsarExamplesTest {

    @Inject
    example.Producer producer;
    @Inject
    ConsumerProducer consumerProducer;
    @Inject
    ReaderExample readerExample;
    @Inject
    PulsarClient pulsarClient;

    Consumer<String> subscribe(String topic, String subscriptionName) throws PulsarClientException {
        return pulsarClient.newConsumer(new StringSchema())
            .topic(topic)
            .subscriptionName(subscriptionName)
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe();
    }

    /**
     * Receives messages until every expected value has been received (other tests publish to the same topics).
     */
    void receive(Consumer<String> consumer, List<String> expected) throws PulsarClientException {
        List<String> received = new ArrayList<>();
        while (!received.containsAll(expected)) {
            Message<String> message = consumer.receive(60, TimeUnit.SECONDS);
            assertNotNull(message, "Expected " + expected + " but received " + received);
            consumer.acknowledge(message);
            received.add(message.getValue());
        }
    }

    @Test
    void testProducer() throws Exception {
        try (Consumer<String> messages = subscribe("persistent://public/default/messages-java-docs", "pulsar-jtest-messages")) {
            MessageId messageId = producer.send("hello").get(30, TimeUnit.SECONDS);
            assertNotNull(messageId);
            producer.sendBlocking("world");

            receive(messages, List.of("hello", "world"));
        }
    }

    @Test
    void testProducerMethodOfABean() throws Exception {
        try (Consumer<String> reports = subscribe("persistent://public/default/reports-java-docs", "pulsar-jtest-reports")) {
            assertEquals("Reporting message report", consumerProducer.report("report").get(30, TimeUnit.SECONDS));

            receive(reports, List.of("report"));
        }
    }

    @Test
    void testConsumer() throws Exception {
        try (Consumer<String> reports = subscribe("persistent://public/default/reports-java-docs", "pulsar-jtest-consumer-reports")) {
            producer.sendBlocking("consumed");

            receive(reports, List.of("consumed"));
        }
    }

    @Test
    void testReader() throws Exception {
        try (Producer<String> messages = pulsarClient.newProducer(new StringSchema())
            .topic("persistent://public/default/messages")
            .producerName("java-reader-test-producer")
            .create()) {
            MessageId messageId = messages.send("read me");
            Message<String> message = readerExample.readNext().get(60, TimeUnit.SECONDS);
            assertNotNull(message);
            assertEquals(messageId, message.getMessageId());
            assertEquals("read me", message.getValue());
        }
    }
}
