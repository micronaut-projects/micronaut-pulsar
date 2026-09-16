from java.util.concurrent import CompletableFuture
from micronaut.pulsar.annotation import PulsarConsumer, PulsarProducer, PulsarSubscription
from org.apache.pulsar.client.api import SubscriptionType


@PulsarSubscription(subscriptionName="pulsar-pytest-subscription", subscriptionType=SubscriptionType.Shared)  # <1>
class ConsumerProducer:  # <2>

    # TODO(python): @PulsarConsumer methods are not supported by the Python compiler yet (treated as @Bean factory methods)
    # @PulsarConsumer(topic="persistent://public/default/messages-python-docs", consumerName="shared-consumer-pytester")  # <3>
    def message_printer(self, message: str) -> None:  # <4>
        changed = self.report(message).get()
        # ...

    @PulsarProducer(topic="persistent://public/default/reports-python-docs", producerName="report-producer-python")  # <5>
    def report(self, message: str) -> CompletableFuture[str]:  # <6>
        return CompletableFuture.supplyAsync(lambda: f"Reporting message {message}")  # <7>
