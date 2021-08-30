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
import io.micronaut.context.env.Environment
import io.micronaut.pulsar.annotation.PulsarProducer
import io.micronaut.pulsar.annotation.PulsarProducerClient
import kotlinexample.dto.PulsarMessage
import kotlinexample.listeners.ReportsTracker
import kotlinx.coroutines.flow.first
import org.apache.pulsar.client.api.MessageId
import java.time.LocalDateTime

class AppTest() : BehaviorSpec({
    val specName = javaClass.simpleName
    given("A producer and a message") {
        val ctx = startContext(specName)
        val producer = ctx.getBean(ProducerHolder::class.java)
        if (!PulsarWrapper.isRunning()) throw Exception("Pulsar failed to start")
        val subscription = ctx.getBean(ReportsTracker::class.java).subscribe()
        val message = PulsarMessage(LocalDateTime.now().toString(), "test")
        `when`("Message is sent") {
            val messageId = producer.send(message)
            then("Message should arrive at stream endpoint") {
                val received = subscription.first()
                message.toMessage(messageId) shouldBeEqualIgnoringCase received
            }
        }
    }
}) {
    companion object {

        fun startContext(specName: String): ApplicationContext {
            return ApplicationContext.run(mapOf(
                    "pulsar.service-url" to PulsarWrapper.pulsarBroker,
                    "spec.name" to specName
            ), Environment.TEST)
        }

        @PulsarProducerClient
        interface ProducerHolder {
            @PulsarProducer(topic = "persistent://public/default/messages-kotlin-docs", producerName = "kotlin-test-producer")
            suspend fun send(message: PulsarMessage): MessageId
        }
    }
}

