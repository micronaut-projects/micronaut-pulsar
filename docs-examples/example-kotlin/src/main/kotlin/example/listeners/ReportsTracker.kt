package example.listeners

import io.micronaut.pulsar.annotation.PulsarConsumer
import io.micronaut.pulsar.annotation.PulsarSubscription
import io.reactivex.subjects.PublishSubject
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.rx2.awaitFirstOrNull
import org.apache.pulsar.client.api.SubscriptionType

@PulsarSubscription(subscriptionName = "kreports", subscriptionType = SubscriptionType.Shared)
class ReportsTracker {

    private val messageTracker = PublishSubject.create<String>()

    init {
        messageTracker.subscribe()
    }

    @PulsarConsumer(consumerName = "report-listener", topic = "persistent://public/default/reports", subscribeAsync = false)
    suspend fun report(message: String) { //suspend to enable non-blocking approach for pulsar receiver
        messageTracker.onNext(message)
        GlobalScope.coroutineContext
    }

    suspend fun latest() = messageTracker.awaitFirstOrNull()
}