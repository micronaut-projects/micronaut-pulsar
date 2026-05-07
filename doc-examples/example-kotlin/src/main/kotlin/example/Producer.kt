package example

import io.micronaut.pulsar.annotation.PulsarProducer
import io.micronaut.pulsar.annotation.PulsarProducerClient
import org.apache.pulsar.client.api.MessageId

@PulsarProducerClient // <1>
interface Producer {
    @PulsarProducer(topic = "persistent://public/default/messages-kotlin-docs", producerName = "kotlin-test-producer") // <2>
    suspend fun send(message: String): MessageId // <3>
    @PulsarProducer(topic = "persistent://public/default/messages-kotlin-docs", producerName = "b-kotlin-test-producer")
    fun sendBlocking(message: String) // <4>
}

