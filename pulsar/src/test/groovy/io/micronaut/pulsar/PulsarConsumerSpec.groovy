/*
 * Copyright 2021 original authors
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
package io.micronaut.pulsar

import groovy.transform.CompileStatic
import io.micronaut.pulsar.annotation.PulsarConsumer
import io.micronaut.pulsar.annotation.PulsarSubscription
import org.apache.pulsar.client.api.*
import spock.lang.Stepwise
import spock.util.concurrent.PollingConditions

import java.util.concurrent.TimeUnit

@Stepwise
class PulsarConsumerSpec extends PulsarAwareTest {

    //topic not listened to explicitly
    static final String PULSAR_REGEX_TEST_TOPIC = "persistent://public/default/other2"
    static final String PULSAR_STATIC_TOPIC_TEST = "persistent://public/default/test"

    static {
        PulsarDefaultContainer.createNonPartitionedTopic(PulsarConsumerSpec.PULSAR_REGEX_TEST_TOPIC)
        PulsarDefaultContainer.createNonPartitionedTopic(PulsarConsumerSpec.PULSAR_STATIC_TOPIC_TEST)
    }

    void "test create consumer beans"() {
        expect:
        context.isRunning()
        context.containsBean(PulsarConsumerTopicListTester)
        PulsarDefaultContainer.PULSAR_ADMIN.topics().getList("public/default").findAll {
            it.contains("test") || it.contains("other2")
        }.size() == 2
    }

    void "test consumer read default topic"() {
        when:
        def consumerTester = context.getBean(PulsarConsumerTopicListTester)
        def producer = context.getBean(PulsarClient).newProducer()
                .topic(PulsarConsumerSpec.PULSAR_STATIC_TOPIC_TEST)
                .producerName("test-producer").create()
        //simple consumer with topic list and blocking
        def message = "This should be received"
        def messageId = producer.send(message.bytes)
        PollingConditions pollingConditions = new PollingConditions(timeout: 60, delay: 1)

        then:
        pollingConditions.eventually {
            message == consumerTester.latestMessage
            messageId == consumerTester.latestMessageId
        }

        cleanup:
        producer.close()
    }

    void "test defined schema consumer read async with regex"() {
        when:
        PulsarConsumerTopicPatternTester consumerPatternTester = context.getBean(PulsarConsumerTopicPatternTester)
        context.destroyBean(PulsarConsumerTopicListTester)
        Producer<String> producer = context.getBean(PulsarClient).newProducer(Schema.STRING).topic(PulsarConsumerSpec.PULSAR_REGEX_TEST_TOPIC).create()
        def blockingReader = context.getBean(PulsarClient).newReader(Schema.STRING).startMessageId(MessageId.latest)
                .topic(PulsarConsumerSpec.PULSAR_REGEX_TEST_TOPIC)
                .create()
        String message = "This should be received"
        PollingConditions conditions = new PollingConditions(timeout: 65, delay: 1)
        MessageId messageId = producer.send(message)

        then:
        Message<String> controlMessage = blockingReader.readNext(10, TimeUnit.SECONDS)
        null != messageId
        messageId == controlMessage.messageId
        conditions.eventually {
            message == consumerPatternTester.latestMessage
            messageId == consumerPatternTester.latestMessageId
        }

        cleanup:
        producer.close()
    }

    @PulsarSubscription(subscriptionName = "array-subscriber-non-async")
    static class PulsarConsumerTopicListTester {
        String latestMessage
        MessageId latestMessageId
        Consumer<byte[]> latestConsumer

        //testing reverse order to ensure processor will do correct call
        @PulsarConsumer(topics = ['persistent://public/default/test'], consumerName = 'single-topic-consumer', subscribeAsync = false)
        def topicListener(Message<byte[]> message, Consumer<byte[]> consumer) {
            latestMessageId = message.messageId
            latestMessage = new String(message.getValue())
            latestConsumer = consumer
        }
    }

    @PulsarSubscription(subscriptionName = "subscribe-2-listeners")
    static class PulsarConsumerTopicPatternTester {
        String latestMessage
        Consumer<String> latestConsumer
        MessageId latestMessageId

        //testing default order
        //fails to subscribe to test topic because exclusive consumer is connected already so subscribe only to other
        @PulsarConsumer(topicsPattern = 'persistent://public/default/other.*', consumerName = "consumer-async")
        def asyncTopicListener(Consumer<String> consumer, Message<String> message) {
            latestMessage = message.value
            latestConsumer = consumer
            latestMessageId = message.messageId
        }
    }
}