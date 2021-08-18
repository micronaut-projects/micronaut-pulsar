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
package kotlinexample.listeners

import io.micronaut.pulsar.annotation.PulsarConsumer
import io.micronaut.pulsar.annotation.PulsarSubscription
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ConflatedBroadcastChannel
import kotlinx.coroutines.flow.receiveAsFlow
import org.apache.pulsar.client.api.SubscriptionType

@PulsarSubscription(subscriptionName = "kreports", subscriptionType = SubscriptionType.Shared)
class ReportsTracker : AutoCloseable {

    @ExperimentalCoroutinesApi
    private val messageTracker = ConflatedBroadcastChannel<String>()

    @ExperimentalCoroutinesApi
    @PulsarConsumer(consumerName = "report-listener-kotlin", topic = "persistent://public/default/reports-kotlin-docs", subscribeAsync = false)
    suspend fun report(message: String) {
        messageTracker.send(message)
    }

    @ExperimentalCoroutinesApi
    fun latest() = messageTracker.openSubscription()

    @ExperimentalCoroutinesApi
    override fun close() {
        messageTracker.close()
    }
}