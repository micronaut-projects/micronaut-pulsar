package example

import io.micronaut.pulsar.annotation.PulsarConsumer
import io.micronaut.pulsar.annotation.PulsarProducer
import io.micronaut.pulsar.annotation.PulsarSubscription
import org.apache.pulsar.client.api.SubscriptionType

@PulsarSubscription(subscriptionName = "pulsar-ktest-subscription", subscriptionType = SubscriptionType.Shared) // <1>
open class ConsumerProducer { // <2>

    @PulsarConsumer(topic = "persistent://public/default/messages-kotlin-docs", consumerName = "shared-consumer-ktester") // <3>
    suspend fun messagePrinter(message: String) { // <4>
        val changed = report(message)
        //...
    }


    @PulsarProducer(topic = "persistent://public/default/reports-kotlin-docs", producerName = "report-producer-kotlin") // <5>
    open suspend fun report(message: String): String { // <6>
        return "Reporting message '$message'" // <7>
    }
}