package example.kotlin

import example.kotlin.dto.PulsarMessage
import io.micronaut.pulsar.annotation.PulsarProducer
import io.micronaut.pulsar.annotation.PulsarProducerClient
import org.apache.pulsar.client.api.MessageId

@PulsarProducerClient
interface TestProducer {
    @PulsarProducer(topic = "persistent://public/default/messages-kotlin-docs")
    suspend fun produce(message: PulsarMessage): MessageId
}