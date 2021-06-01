package example.listeners

import example.dto.PulsarMessage
import io.micronaut.context.annotation.Context
import io.micronaut.pulsar.annotation.PulsarConsumer
import io.micronaut.pulsar.annotation.PulsarProducer
import io.micronaut.pulsar.annotation.PulsarSubscription
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.SubscriptionType
import org.slf4j.LoggerFactory

@PulsarSubscription(subscriptionName = "pulsar-ktest-subscription", subscriptionType = SubscriptionType.Shared)
@Context
open class MessagingService {
    private val logger = LoggerFactory.getLogger(MessagingService::class.java)

    @PulsarConsumer(topic = "persistent://public/default/messages", consumerName = "shared-consumer-ktester", subscribeAsync = false)
    suspend fun messagePrinter(message: Message<PulsarMessage>) {
        logger.info("A message was received {}. Sent on {}", message.value.message, message.value.sent)
        report(message.value.toMessage(message.messageId))
    }


    @PulsarProducer(topic = "persistent://public/default/reports", producerName = "report-producer-kotlin")
    open suspend fun report(message: String): String {
        logger.info("Sending message $message")
        return message
    }
}