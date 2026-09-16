from typing import Annotated

from jakarta.inject import Inject
from java.util.concurrent import TimeUnit
from micronaut.test.extensions.junit5.annotation import MicronautTest
from org.apache.pulsar.client.api import Consumer, PulsarClient, SubscriptionInitialPosition
from org.apache.pulsar.client.impl.schema import StringSchema
from org.junit.jupiter.api import Disabled, Test

from example.ConsumerProducer import ConsumerProducer
from example.Producer import Producer
from example.ReaderExample import ReaderExample


@MicronautTest(environments=["pulsar"])
class PulsarExamplesTest:
    producer: Annotated[Producer, Inject]
    consumer_producer: Annotated[ConsumerProducer, Inject]
    reader_example: Annotated[ReaderExample, Inject]
    pulsar_client: Annotated[PulsarClient, Inject]

    def subscribe(self, topic: str, subscription_name: str) -> Consumer[str]:
        return (self.pulsar_client.newConsumer(StringSchema())
                .topic(topic)
                .subscriptionName(subscription_name)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe())

    def receive(self, consumer: Consumer[str], expected: list[str]) -> None:
        """Receives messages until every expected value has been received (other tests publish to the same topics)."""
        received = []
        while not all(value in received for value in expected):
            message = consumer.receive(60, TimeUnit.SECONDS)
            assert message is not None, f"Expected {expected} but received {received}"
            consumer.acknowledge(message)
            received.append(str(message.getValue()))

    @Test
    def test_producer(self) -> None:
        messages = self.subscribe("persistent://public/default/messages-python-docs", "pulsar-pytest-messages")
        try:
            message_id = self.producer.send("hello").get(30, TimeUnit.SECONDS)
            assert message_id is not None
            self.producer.send_blocking("world")

            self.receive(messages, ["hello", "world"])
        finally:
            messages.close()

    @Test
    def test_producer_method_of_a_bean(self) -> None:
        reports = self.subscribe("persistent://public/default/reports-python-docs", "pulsar-pytest-reports")
        try:
            assert str(self.consumer_producer.report("report").get(30, TimeUnit.SECONDS)) == "Reporting message report"

            self.receive(reports, ["report"])
        finally:
            reports.close()

    # TODO(python): the @PulsarConsumer decorator of ConsumerProducer.message_printer is commented out because the Python
    # compiler treats the method as a @Bean factory method, so no consumer is subscribed and nothing is reported
    @Disabled("TODO(python): @PulsarConsumer methods are treated as @Bean factory methods by the Python compiler")
    @Test
    def test_consumer(self) -> None:
        reports = self.subscribe("persistent://public/default/reports-python-docs", "pulsar-pytest-consumer-reports")
        try:
            self.producer.send_blocking("consumed")

            self.receive(reports, ["consumed"])
        finally:
            reports.close()

    @Test
    def test_reader(self) -> None:
        messages = (self.pulsar_client.newProducer(StringSchema())
                    .topic("persistent://public/default/messages")
                    .producerName("python-reader-test-producer")
                    .create())
        try:
            message_id = messages.send("read me")
            message = self.reader_example.read_next().get(60, TimeUnit.SECONDS)
            assert message is not None
            assert str(message.getMessageId()) == str(message_id)
            assert str(message.getValue()) == "read me"
        finally:
            messages.close()
