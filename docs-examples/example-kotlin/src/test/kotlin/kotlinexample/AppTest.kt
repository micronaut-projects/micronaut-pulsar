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
import io.kotest.matchers.string.shouldBeEqualIgnoringCase
import io.micronaut.context.ApplicationContext
import io.micronaut.pulsar.annotation.PulsarProducer
import io.micronaut.pulsar.annotation.PulsarProducerClient
import io.micronaut.pulsar.annotation.PulsarServiceUrlProvider
import io.micronaut.test.extensions.kotest.annotation.MicronautTest
import jakarta.inject.Singleton
import kotlinexample.dto.PulsarMessage
import kotlinexample.listeners.ReportsTracker
import kotlinx.coroutines.flow.first
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.ServiceUrlProvider
import java.time.LocalDateTime

@MicronautTest
open class AppTest(private val context: ApplicationContext, private val tracker: ReportsTracker) : BehaviorSpec({
    given("message") {
        if (!PulsarWrapper.isRunning()) throw Exception("Pulsar failed to start")
        val subscription = tracker.subscribe()
        val producer = context.getBean(ProducerHolder::class.java)
        val message = PulsarMessage(LocalDateTime.now().toString(), "test")
        `when`("is sent") {
            val messageId = producer.send(message)
            then("should stream latest") {
                val received = subscription.first()
                message.toMessage(messageId) shouldBeEqualIgnoringCase received
            }
        }
    }
}) {

    @PulsarProducerClient
    interface ProducerHolder {
        @PulsarProducer(topic = "persistent://public/default/messages-kotlin-docs", producerName = "kotlin-test-producer")
        suspend fun send(message: PulsarMessage): MessageId
    }

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

