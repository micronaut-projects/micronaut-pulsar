/*
 * Copyright 2017-2022 original authors
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
import io.micronaut.messaging.annotation.MessageBody
import io.micronaut.messaging.annotation.MessageHeader
import io.micronaut.messaging.annotation.MessageMapping
import io.micronaut.pulsar.PulsarAwareTest
import io.micronaut.pulsar.annotation.MessageProperties
import io.micronaut.pulsar.annotation.PulsarConsumer
import io.micronaut.pulsar.annotation.PulsarSubscription
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Singleton
import org.apache.pulsar.client.api.*
import org.apache.pulsar.client.impl.schema.StringSchema
import reactor.core.publisher.Mono
import spock.lang.Stepwise
import spock.util.concurrent.BlockingVariables

import static java.util.concurrent.TimeUnit.SECONDS
import static org.apache.pulsar.client.api.MessageId.latest

@Stepwise
class PulsarConsumerSpec extends PulsarAwareTest {

    public static final String PULSAR_REGEX_TEST_TOPIC = "persistent://public/default/other2"
    public static final String PULSAR_STATIC_TOPIC_TEST = "persistent://public/default/test"

    void "test consumer read default topic and array"() {
        given:
        BlockingVariables varsSingle = new BlockingVariables(65)
        BlockingVariables vars = new BlockingVariables(65)
        BlockingVariables varsHeader = new BlockingVariables(65)

        when:
        PulsarConsumerTopicTester singleTopicTester = context.getBean(PulsarConsumerTopicTester.class)
        PulsarConsumerTopicListTester arrayTester = context.getBean(PulsarConsumerTopicListTester.class)
        PulsarConsumerHeaderTester headerTester = context.getBean(PulsarConsumerHeaderTester.class)
        PulsarConsumerHeadersTester headersTester = context.getBean(PulsarConsumerHeadersTester.class)
        arrayTester.blockers = vars
        singleTopicTester.blockers = varsSingle
        headerTester.blockers = varsHeader
        headersTester.blockers = varsHeader
        Producer producer = context.getBean(PulsarClient)
                .newProducer()
                .topic(PulsarConsumerSpec.PULSAR_STATIC_TOPIC_TEST)
                .producerName("test-producer-simple")
                .create()
        //simple consumer with topic list and blocking
        String message = "This should be received"
        MessageId messageId = producer.newMessage().value(message.bytes).property("header", "test").send()

        then:
        null != messageId
        messageId == vars.getProperty("messageId")
        messageId == varsSingle.getProperty("messageId")
        message == vars.getProperty("value")
        message == varsSingle.getProperty("value")
        null != varsSingle.getProperty("consumer")
        varsHeader.getProperty("property") == "test"
        varsHeader.getProperty("properties") ?["header"] == "test"

        cleanup:
        producer.close()
    }

    void "test defined schema consumer read async with regex"() {
        given:
        BlockingVariables variables = new BlockingVariables(65)

        when:
        Producer<String> producer = context.getBean(PulsarClient)
                .newProducer(new StringSchema())
                .producerName("simple-producer-regex")
                .topic(PulsarConsumerSpec.PULSAR_REGEX_TEST_TOPIC)
                .create()
        Reader blockingReader = context.getBean(PulsarClient)
                .newReader(new StringSchema())
                .readerName("simple-reader-blocker")
                .startMessageId(latest)
                .topic(PulsarConsumerSpec.PULSAR_REGEX_TEST_TOPIC)
                .create()
        def consumerPatternTester = context.getBean(PulsarConsumerTopicPatternTester)
        consumerPatternTester.blockers = variables
        String message = "This should be received"
        MessageId messageId = producer.send(message)

        then:
        Message<String> controlMessage = blockingReader.readNext(10, SECONDS)
        messageId == controlMessage.messageId
        message == variables.getProperty('latestMessage')
        messageId == variables.getProperty('latestMessageId')

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
        annotationValue.get().contains 'persistent://public/default/test'

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
    @PulsarSubscription(subscriptionName = "subscriber-simple-exclusive")
    static class PulsarConsumerTopicTester {
        BlockingVariables blockers

        @PulsarConsumer(
                topic = PulsarConsumerSpec.PULSAR_STATIC_TOPIC_TEST,
                consumerName = 'simple-topic-consumer',
                subscribeAsync = false)
        void topicListener(@MessageBody Message<byte[]> message, Consumer<byte[]> consumer) {
            if (null == blockers) {
                return
            }
            blockers.setProperty("messageId", message.messageId)
            blockers.setProperty("value", new String(message.value))
            blockers.setProperty("consumer", consumer)
        }
    }

    @Requires(property = 'spec.name', value = 'PulsarConsumerSpec')
    @PulsarSubscription(subscriptionName = "subscriber-simple-header")
    static class PulsarConsumerHeaderTester {

        BlockingVariables blockers

        @PulsarConsumer(
                topic = PulsarConsumerSpec.PULSAR_STATIC_TOPIC_TEST,
                consumerName = 'simple-header-consumer2',
                subscribeAsync = false)
        void propertyListener(@MessageBody byte[] message, @MessageHeader("header") String header) {
            if (null == blockers) {
                return
            }
            blockers.setProperty("property", header)
        }
    }

    @Requires(property = 'spec.name', value = 'PulsarConsumerSpec')
    @PulsarSubscription(subscriptionName = "subscriber-simple-headers")
    static class PulsarConsumerHeadersTester {

        BlockingVariables blockers

        @PulsarConsumer(
                topic = PulsarConsumerSpec.PULSAR_STATIC_TOPIC_TEST,
                consumerName = 'simple-headers-consumer',
                subscribeAsync = false)
        void propertiesListener(@MessageBody byte[] message, @MessageProperties Map<String, String> headers) {
            if (null == blockers) {
                return
            }
            blockers.setProperty("properties", headers)
        }
    }


    @Requires(property = 'spec.name', value = 'PulsarConsumerSpec')
    @PulsarSubscription(subscriptionName = "array-subscriber-non-async")
    static class PulsarConsumerTopicListTester {
        BlockingVariables blockers

        //testing reverse order to ensure processor will do correct call
        @PulsarConsumer(
                topics = [PulsarConsumerSpec.PULSAR_STATIC_TOPIC_TEST],
                consumerName = 'single-topic-consumer',
                subscribeAsync = false)
        void topicListener(@MessageBody Message<byte[]> message, Consumer<byte[]> consumer) {
            //due to usage of the same topic on above consumers prevent this method unless blockers are set
            //which indicates that the test running is meant for this listener
            if (null == blockers) {
                return
            }
            blockers.setProperty("messageId", message.messageId)
            blockers.setProperty("value", new String(message.value))
        }
    }

    @Requires(property = 'spec.name', value = 'PulsarConsumerSpec')
    @PulsarSubscription(subscriptionName = "subscribe-regex-async", subscriptionType = SubscriptionType.Shared)
    static class PulsarConsumerTopicPatternTester {

        BlockingVariables blockers

        //testing default order
        //fails to subscribe to test topic because exclusive consumer is connected already so subscribe only to other
        @PulsarConsumer(topicsPattern = 'persistent://public/default/other.*', consumerName = "consumer-async", patternAutoDiscoveryPeriod = 10)
        Mono<Void> asyncTopicListener(Consumer<String> consumer, @MessageBody Message<String> message) {
            if (null == blockers) {
                return
            }
            blockers.setProperty('latestMessage', message.value)
            blockers.setProperty('latestMessageId', message.messageId)
        }
    }
}
