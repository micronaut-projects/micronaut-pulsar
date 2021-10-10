package example;

import io.micronaut.pulsar.annotation.PulsarReader;
import jakarta.inject.Singleton;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Reader;

import java.util.concurrent.CompletableFuture;


@Singleton
public class ReaderExample {

    @PulsarReader(value = "persistent://public/default/messages", readerName = "simple-j-reader") // <1>
    private Reader<String> reader; // <2>

    public CompletableFuture<Message<String>> readNext() { // <3>
        return reader.readNextAsync(); // <4>
    }
}
