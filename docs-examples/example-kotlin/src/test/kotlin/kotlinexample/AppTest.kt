/*
 *
 *  * Copyright 2017-2021 original authors
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * https://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package kotlinexample

import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.string.shouldBeEqualIgnoringCase
import io.micronaut.pulsar.annotation.PulsarProducer
import io.micronaut.pulsar.annotation.PulsarServiceUrlProvider
import io.micronaut.test.extensions.kotest.annotation.MicronautTest
import kotlinexample.dto.PulsarMessage
import kotlinexample.listeners.ReportsTracker
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.ServiceUrlProvider
import java.time.LocalDateTime
import javax.inject.Named
import javax.inject.Singleton

@ExperimentalCoroutinesApi
@MicronautTest
abstract class AppTest(@Named private val producer: Producer<PulsarMessage>, private val tracker: ReportsTracker) : BehaviorSpec({
    given("message") {
        if (!PulsarWrapper.isRunning()) throw Exception("Pulsar failed to start")
        val subscription = tracker.latest()
        val message = PulsarMessage(LocalDateTime.now().toString(), "test")
        `when`("is sent") {
            val messageId = producer.send(message)
            then("should stream latest") {
                val received = subscription.receive().shouldNotBeNull()
                message.toMessage(messageId) shouldBeEqualIgnoringCase received
            }
        }
    }
}) {

    @PulsarProducer(topic = "persistent://public/default/messages-kotlin-docs")
    abstract fun testProducer(): Producer<PulsarMessage>

    @Singleton
    @PulsarServiceUrlProvider
    fun urlProvider(): ServiceUrlProvider = object : ServiceUrlProvider {
        private var pulsarClient: PulsarClient? = null

        override fun initialize(client: PulsarClient) {
            pulsarClient = client
        }

        override fun getServiceUrl(): String = PulsarWrapper.pulsarBroker

    }
}

