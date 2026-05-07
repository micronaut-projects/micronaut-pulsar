package example;

import io.micronaut.pulsar.annotation.PulsarReader;
import jakarta.inject.Singleton;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Reader;

import java.util.concurrent.CompletableFuture;


@Singleton
class ReaderExample {

    @PulsarReader(value = "persistent://public/default/messages", readerName = "simple-g-reader") // <1>
    private Reader<String> reader // <2>

    CompletableFuture<Message<String>> readNext() { // <3>
        return reader.readNextAsync() // <4>
    }
}
