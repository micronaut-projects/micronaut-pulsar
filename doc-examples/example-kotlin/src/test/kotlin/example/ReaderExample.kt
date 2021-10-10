package example

import io.micronaut.pulsar.annotation.PulsarReader
import jakarta.inject.Singleton
import kotlinx.coroutines.future.await
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Reader

@Singleton
class ReaderExample {
    @PulsarReader(value = "persistent://public/default/messages", readerName = "simple-k-reader") // <1>
    private lateinit var reader: Reader<String> // <2>

    suspend fun readNext(): Message<String> { // <3>
        return reader.readNextAsync().await() // <4>
    }
}