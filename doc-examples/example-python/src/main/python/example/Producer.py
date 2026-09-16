from abc import ABC, abstractmethod

from java.util.concurrent import CompletableFuture
from micronaut.pulsar.annotation import PulsarProducer, PulsarProducerClient
from org.apache.pulsar.client.api import MessageId


@PulsarProducerClient  # <1>
class Producer(ABC):

    @PulsarProducer(topic="persistent://public/default/messages-python-docs", producerName="python-test-producer")  # <2>
    @abstractmethod
    def send(self, message: str) -> CompletableFuture[MessageId]:  # <3>
        ...

    @PulsarProducer(topic="persistent://public/default/messages-python-docs", producerName="b-python-test-producer")
    @abstractmethod
    def send_blocking(self, message: str) -> None:  # <4>
        ...
