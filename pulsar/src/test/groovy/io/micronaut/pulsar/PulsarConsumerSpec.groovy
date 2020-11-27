/*
 * Copyright 2017-2020 original authors
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

import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import io.micronaut.core.util.StringUtils
import io.micronaut.pulsar.annotation.PulsarConsumer
import io.micronaut.pulsar.annotation.PulsarSubscription
import io.micronaut.pulsar.config.PulsarClientConfiguration
import io.micronaut.pulsar.processor.PulsarConsumerProcessor
import io.micronaut.runtime.server.EmbeddedServer
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.*
import org.testcontainers.containers.PulsarContainer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise
import spock.util.concurrent.PollingConditions

import java.util.concurrent.TimeUnit

@Stepwise
class PulsarConsumerSpec extends Specification {

    @AutoCleanup
    @Shared
    PulsarContainer pulsarContainer = new PulsarContainer("2.6.2")

    @Shared
    @AutoCleanup
    ApplicationContext context

    @Shared
    @AutoCleanup
    EmbeddedServer embeddedServer

    def setupSpec() {
        pulsarContainer.start()
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarContainer.httpServiceUrl).build()
        admin.topics().createNonPartitionedTopic("public/default/other2")
        embeddedServer = ApplicationContext.run(EmbeddedServer,
                CollectionUtils.mapOf("pulsar.service-url", pulsarContainer.pulsarBrokerUrl,
                        "pulsar.io-threads", 2, "listener-threads", 2),
                StringUtils.EMPTY_STRING_ARRAY
        )
        context = embeddedServer.applicationContext
    }

    void "test load configuration"() {
        expect:
        context.isRunning()
        context.containsBean(PulsarClientConfiguration)
        context.containsBean(PulsarClient)
        context.containsBean(PulsarConsumerProcessor)
        context.containsBean(PulsarConsumerTopicListTester)
        pulsarContainer.pulsarBrokerUrl == context.getBean(PulsarClientConfiguration).serviceUrl
    }

    void "test consumer read"() {
        when:
        def topic = "persistent://public/default/test"
        def consumerTester = context.getBean(PulsarConsumerTopicListTester)
        def producer = context.getBean(PulsarClient).newProducer().topic(topic).producerName("test-producer").create()
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

    void "test defined schema pattern consumer read with async"() {
        when:
        String topic = 'persistent://public/default/other2'
        PulsarConsumerTopicPatternTester consumerPatternTester = context.getBean(PulsarConsumerTopicPatternTester)
        context.destroyBean(PulsarConsumerTopicListTester)
        Producer<String> producer = context.getBean(PulsarClient).newProducer(Schema.STRING).topic(topic).create()
        def blockingReader = context.getBean(PulsarClient).newReader(Schema.STRING).startMessageId(MessageId.latest)
                .topic(topic)
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

    @PulsarSubscription
    static class PulsarConsumerTopicListTester {
        String latestMessage
        MessageId latestMessageId
        Consumer<byte[]> latestConsumer

        PulsarConsumerTopicListTester() {
        }

        //testing reverse order to ensure processor will do correct call
        @PulsarConsumer(topics = ["persistent://public/default/test"], consumerName = "single-topic-consumer", subscribeAsync = false)
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