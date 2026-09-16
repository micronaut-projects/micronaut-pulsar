from typing import Annotated

from jakarta.inject import Inject, Singleton
from java.util.concurrent import CompletableFuture
from micronaut.pulsar.annotation import PulsarReader
from org.apache.pulsar.client.api import Message, Reader


@Singleton
class ReaderExample:

    reader: Annotated[Reader[str], Inject, PulsarReader("persistent://public/default/messages", readerName="simple-py-reader")]  # <1> <2>

    def read_next(self) -> CompletableFuture[Message[str]]:  # <3>
        return self.reader.readNextAsync()  # <4>
