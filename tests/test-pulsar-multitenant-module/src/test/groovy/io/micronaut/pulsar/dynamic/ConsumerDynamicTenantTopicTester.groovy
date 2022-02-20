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
package io.micronaut.pulsar.dynamic

import io.micronaut.context.annotation.Requires
import io.micronaut.messaging.annotation.MessageBody
import io.micronaut.pulsar.DynamicTenantTopicSpec
import io.micronaut.pulsar.annotation.PulsarConsumer
import io.micronaut.pulsar.annotation.PulsarSubscription
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import spock.util.concurrent.BlockingVariables

@Requires(property = 'spec.name', value = 'DynamicTenantTopicSpec')
@PulsarSubscription(subscriptionName = "subscriber-dynamic")
class ConsumerDynamicTenantTopicTester {
    BlockingVariables blockers

    @PulsarConsumer(
            topic = DynamicTenantTopicSpec.PULSAR_DYNAMIC_TENANT_TEST_TOPIC,
            consumerName = 'dynamic-topic-consumer',
            subscribeAsync = false)
    void topicListener(@MessageBody Message<String> message, Consumer<String> consumer) {
        if (null == blockers) {
            return
        }
        if (null == blockers.getProperty("messages")) {
            blockers.setProperty("messages", [[:]])
        }
        blockers.getProperty("messages")["topic"] = consumer.topic
        blockers.getProperty("messages")["messageId"] = message.messageId
        blockers.getProperty("messages")["value"] = new String(message.value)
    }
}
