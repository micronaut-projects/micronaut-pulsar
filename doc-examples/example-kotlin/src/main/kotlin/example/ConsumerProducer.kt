package example

import io.micronaut.pulsar.annotation.PulsarConsumer
import io.micronaut.pulsar.annotation.PulsarProducer
import io.micronaut.pulsar.annotation.PulsarSubscription
import org.apache.pulsar.client.api.SubscriptionType
import java.util.concurrent.CompletableFuture

@PulsarSubscription(subscriptionName = "pulsar-ktest-subscription", subscriptionType = SubscriptionType.Shared) // <1>
open class ConsumerProducer { // <2>

    @PulsarConsumer(topic = "persistent://public/default/messages-kotlin-docs", consumerName = "shared-consumer-ktester") // <3>
    fun messagePrinter(message: String) { // <4>
        val changed = report(message).get()
        //...
    }


    @PulsarProducer(topic = "persistent://public/default/reports-kotlin-docs", producerName = "report-producer-kotlin") // <5>
    open fun report(message: String): CompletableFuture<String> { // <6>
        return CompletableFuture.supplyAsync { "Reporting message '$message'" } // <7>
    }
}
