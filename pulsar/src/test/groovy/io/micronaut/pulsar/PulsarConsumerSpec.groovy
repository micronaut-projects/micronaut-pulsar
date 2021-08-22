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
package io.micronaut.pulsar

import io.micronaut.context.annotation.Requires
import io.micronaut.messaging.annotation.MessageMapping
import io.micronaut.pulsar.annotation.PulsarConsumer
import io.micronaut.pulsar.annotation.PulsarSubscription
import io.micronaut.pulsar.shared.PulsarAwareTest
import org.apache.pulsar.client.api.*
import org.apache.pulsar.client.impl.schema.StringSchema
import spock.lang.Stepwise
import spock.util.concurrent.BlockingVariables
import spock.util.concurrent.PollingConditions

import static java.util.concurrent.TimeUnit.SECONDS
import static org.apache.pulsar.client.api.MessageId.latest

@Stepwise
class PulsarConsumerSpec extends PulsarAwareTest {

    public static final String PULSAR_REGEX_TEST_TOPIC = "persistent://public/default/other2"
    public static final String PULSAR_STATIC_TOPIC_TEST = "persistent://public/default/test"

    void "test consumer read default topic"() {
        given:
        BlockingVariables vars = new BlockingVariables(65)

        when:
        PulsarConsumerTopicListTester consumerTester = context.getBean(PulsarConsumerTopicListTester.class)
        consumerTester.blockers = vars
        Producer producer = context.getBean(PulsarClient)
                .newProducer()
                .topic(PULSAR_STATIC_TOPIC_TEST)
                .producerName("test-producer-simple")
                .create()
        //simple consumer with topic list and blocking
        String message = "This should be received"
        MessageId messageId = producer.send(message.bytes)

        then:
        null != messageId
        messageId == vars.getProperty("messageId")
        message == vars.getProperty("value")

        cleanup:
        producer.close()
    }

    void "test defined schema consumer read async with regex"() {
        when:
        def consumerPatternTester = context.getBean(PulsarConsumerTopicPatternTester)
        Producer<String> producer = context.getBean(PulsarClient)
                .newProducer(new StringSchema())
                .producerName("simple-producer-regex")
                .topic(PULSAR_REGEX_TEST_TOPIC)
                .create()
        Reader blockingReader = context.getBean(PulsarClient)
                .newReader(new StringSchema())
                .readerName("simple-reader-blocker")
                .startMessageId(latest)
                .topic(PULSAR_REGEX_TEST_TOPIC)
                .create()
        String message = "This should be received"
        MessageId messageId = producer.send(message)

        then:
        Message<String> controlMessage = blockingReader.readNext(10, SECONDS)
        messageId == controlMessage.messageId
        new PollingConditions(timeout: 65, delay: 1).eventually {
            message == consumerPatternTester.latestMessage
            messageId == consumerPatternTester.latestMessageId
        }

        cleanup:
        producer.close()
        blockingReader.close()
    }

    void "test consumer has MessageMapping annotation with expected topic value"() {
        when:
        def definition = context.getBeanDefinition(PulsarConsumerTopicTester)
        def method = definition.getRequiredMethod('topicListener', Message, Consumer)
        def annotationValue = method.getValue(MessageMapping, String[])

        then:
        annotationValue.isPresent()
        annotationValue.get().contains 'persistent://public/default/simple'

        when:
        definition = context.getBeanDefinition(PulsarConsumerTopicListTester)
        method = definition.getRequiredMethod('topicListener', Message, Consumer)
        annotationValue = method.getValue(MessageMapping, String[])

        then:
        annotationValue.isPresent()
        annotationValue.get().contains 'persistent://public/default/test'

        when:
        definition = context.getBeanDefinition(PulsarConsumerTopicPatternTester)
        method = definition.getRequiredMethod('asyncTopicListener', Consumer, Message)
        annotationValue = method.getValue(MessageMapping, String[])

        then:
        annotationValue.isPresent()
        annotationValue.get().contains 'persistent://public/default/other.*'
    }

    @Requires(property = 'spec.name', value = 'PulsarConsumerSpec')
    @PulsarSubscription(subscriptionName = "subscriber-simple")
    static class PulsarConsumerTopicTester {

        String latestMessage
        MessageId latestMessageId
        Consumer<byte[]> latestConsumer

        @PulsarConsumer(
                topic = 'persistent://public/default/simple',
                consumerName = 'simple-topic-consumer',
                subscribeAsync = false)
        void topicListener(Message<byte[]> message, Consumer<byte[]> consumer) {
            latestMessageId = message.messageId
            latestMessage = new String(message.value)
            latestConsumer = consumer
        }
    }


    @Requires(property = 'spec.name', value = 'PulsarConsumerSpec')
    @PulsarSubscription(subscriptionName = "array-subscriber-non-async")
    static class PulsarConsumerTopicListTester {
        Consumer<byte[]> latestConsumer
        BlockingVariables blockers

        //testing reverse order to ensure processor will do correct call
        @PulsarConsumer(
                topics = [PulsarConsumerSpec.PULSAR_STATIC_TOPIC_TEST],
                consumerName = 'single-topic-consumer',
                subscribeAsync = false)
        void topicListener(Message<byte[]> message, Consumer<byte[]> consumer) {
            blockers.setProperty("messageId", message.messageId)
            blockers.setProperty("value", new String(message.value))
            latestConsumer = consumer
        }
    }

    @Requires(property = 'spec.name', value = 'PulsarConsumerSpec')
    @PulsarSubscription(subscriptionName = "example-java-listeners", subscriptionType = SubscriptionType.Shared)
    static class PulsarConsumerTopicPatternTester {

        String latestMessage
        Consumer<String> latestConsumer
        MessageId latestMessageId

        //testing default order
        //fails to subscribe to test topic because exclusive consumer is connected already so subscribe only to other
        @PulsarConsumer(topicsPattern = 'persistent://public/default/other.*', consumerName = "consumer-async")
        void asyncTopicListener(Consumer<String> consumer, Message<String> message) {
            latestMessage = message.value
            latestConsumer = consumer
            latestMessageId = message.messageId
        }
    }
}
