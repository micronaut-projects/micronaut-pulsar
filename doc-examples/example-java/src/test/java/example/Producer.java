package example;

import io.micronaut.pulsar.annotation.PulsarProducer;
import io.micronaut.pulsar.annotation.PulsarProducerClient;
import org.apache.pulsar.client.api.MessageId;

import java.util.concurrent.CompletableFuture;

@PulsarProducerClient // <1>
public interface Producer {
    @PulsarProducer(topic = "persistent://public/default/messages-kotlin-docs", producerName = "kotlin-test-producer") // <2>
    CompletableFuture<MessageId> send(String message); // <3>

    @PulsarProducer(topic = "persistent://public/default/messages-kotlin-docs", producerName = "b-kotlin-test-producer")
    void sendBlocking(String message); // <4>
}
