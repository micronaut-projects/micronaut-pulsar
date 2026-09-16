package example

import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import kotlinx.coroutines.runBlocking
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.SubscriptionInitialPosition
import org.apache.pulsar.client.impl.schema.StringSchema
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

@MicronautTest(environments = ["pulsar"])
class PulsarExamplesTest {

    @Inject
    lateinit var producer: Producer
    @Inject
    lateinit var consumerProducer: ConsumerProducer
    @Inject
    lateinit var readerExample: ReaderExample
    @Inject
    lateinit var pulsarClient: PulsarClient

    private fun subscribe(topic: String, subscriptionName: String): Consumer<String> =
        pulsarClient.newConsumer(StringSchema())
            .topic(topic)
            .subscriptionName(subscriptionName)
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe()

    /**
     * Receives messages until every expected value has been received (other tests publish to the same topics).
     */
    private fun receive(consumer: Consumer<String>, expected: List<String>) {
        val received = mutableListOf<String>()
        while (!received.containsAll(expected)) {
            val message = consumer.receive(60, TimeUnit.SECONDS)
            assertNotNull(message, "Expected $expected but received $received")
            consumer.acknowledge(message)
            received.add(message.value)
        }
    }

    @Test
    fun testProducer() {
        subscribe("persistent://public/default/messages-kotlin-docs", "pulsar-ktest-messages").use { messages ->
            val messageId = producer.send("hello").get(30, TimeUnit.SECONDS)
            assertNotNull(messageId)
            producer.sendBlocking("world")

            receive(messages, listOf("hello", "world"))
        }
    }

    @Test
    fun testProducerMethodOfABean() {
        subscribe("persistent://public/default/reports-kotlin-docs", "pulsar-ktest-reports").use { reports ->
            assertEquals("Reporting message 'report'", consumerProducer.report("report").get(30, TimeUnit.SECONDS))

            receive(reports, listOf("report"))
        }
    }

    @Test
    fun testConsumer() {
        subscribe("persistent://public/default/reports-kotlin-docs", "pulsar-ktest-consumer-reports").use { reports ->
            producer.sendBlocking("consumed")

            receive(reports, listOf("consumed"))
        }
    }

    @Test
    fun testReader() {
        pulsarClient.newProducer(StringSchema())
            .topic("persistent://public/default/messages")
            .producerName("kotlin-reader-test-producer")
            .create().use { messages ->
                val messageId = messages.send("read me")
                val message = runBlocking { readerExample.readNext() }
                assertNotNull(message)
                assertEquals(messageId, message.messageId)
                assertEquals("read me", message.value)
            }
    }
}
