/*
 * Copyright 2017-2021 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    }

    suspend fun latest() = messageTracker.awaitFirstOrNull()
}