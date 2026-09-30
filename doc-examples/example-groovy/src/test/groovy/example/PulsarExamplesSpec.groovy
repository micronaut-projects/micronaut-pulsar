package example

import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.SubscriptionInitialPosition
import org.apache.pulsar.client.impl.schema.StringSchema
import spock.lang.Specification

import java.util.concurrent.TimeUnit

@MicronautTest(environments = "pulsar")
class PulsarExamplesSpec extends Specification {

    @Inject
    Producer producer
    @Inject
    ConsumerProducer consumerProducer
    @Inject
    ReaderExample readerExample
    @Inject
    PulsarClient pulsarClient

    private Consumer<String> subscribe(String topic, String subscriptionName) {
        pulsarClient.newConsumer(new StringSchema())
            .topic(topic)
            .subscriptionName(subscriptionName)
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe()
    }

    /**
     * Receives messages until every expected value has been received (other tests publish to the same topics).
     */
    private static void receive(Consumer<String> consumer, List<String> expected) {
        List<String> received = []
        while (!received.containsAll(expected)) {
            def message = consumer.receive(60, TimeUnit.SECONDS)
            assert message != null: "Expected $expected but received $received"
            consumer.acknowledge(message)
            received << message.value
        }
    }

    void "test producer"() {
        given:
        Consumer<String> messages = subscribe("persistent://public/default/messages-groovy-docs", "pulsar-gtest-messages")

        when:
        def messageId = producer.send("hello").get(30, TimeUnit.SECONDS)
        producer.sendBlocking("world")

        then:
        messageId != null
        receive(messages, ["hello", "world"])

        cleanup:
        messages.close()
    }

    void "test producer method of a bean"() {
        given:
        Consumer<String> reports = subscribe("persistent://public/default/reports-groovy-docs", "pulsar-gtest-reports")

        when:
        String reported = consumerProducer.report("report").get(30, TimeUnit.SECONDS)

        then:
        reported == "Reporting message report"
        receive(reports, ["report"])

        cleanup:
        reports.close()
    }

    void "test consumer"() {
        given:
        Consumer<String> reports = subscribe("persistent://public/default/reports-groovy-docs", "pulsar-gtest-consumer-reports")

        when:
        producer.sendBlocking("consumed")

        then:
        receive(reports, ["consumed"])

        cleanup:
        reports.close()
    }

    void "test reader"() {
        given:
        def messages = pulsarClient.newProducer(new StringSchema())
            .topic("persistent://public/default/messages")
            .producerName("groovy-reader-test-producer")
            .create()

        when:
        def messageId = messages.send("read me")
        def message = readerExample.readNext().get(60, TimeUnit.SECONDS)

        then:
        message != null
        message.messageId == messageId
        message.value == "read me"

        cleanup:
        messages.close()
    }
}
