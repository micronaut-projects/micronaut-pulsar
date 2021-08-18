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

import kotlinexample.dto.PulsarMessage
import io.micronaut.pulsar.annotation.PulsarConsumer
import io.micronaut.pulsar.annotation.PulsarProducer
import io.micronaut.pulsar.annotation.PulsarSubscription
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.SubscriptionType
import org.slf4j.LoggerFactory

@PulsarSubscription(subscriptionName = "pulsar-ktest-subscription", subscriptionType = SubscriptionType.Shared)
open class MessagingService {
    private val logger = LoggerFactory.getLogger(MessagingService::class.java)

    @PulsarConsumer(topic = "persistent://public/default/messages-kotlin-docs", consumerName = "shared-consumer-ktester", subscribeAsync = false)
    suspend fun messagePrinter(message: Message<PulsarMessage>) {
        logger.info("A message was received {}. Sent on {}", message.value.message, message.value.sent)
        report(message.value.toMessage(message.messageId))
    }


    @PulsarProducer(topic = "persistent://public/default/reports-kotlin-docs", producerName = "report-producer-kotlin")
    open suspend fun report(message: String): String {
        logger.info("Sending message $message")
        return message
    }
}