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

@Stepwise
class PulsarSpec extends Specification {

    @AutoCleanup
    @Shared
    PulsarContainer pulsarContainer = new PulsarContainer("2.6.1")

    @Shared
    @AutoCleanup
    ApplicationContext context

    @Shared
    @AutoCleanup
    EmbeddedServer embeddedServer

    def setupSpec() {
        pulsarContainer.start()
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarContainer.httpServiceUrl).build()
        // due to regex subscription slow updates create topic prior to subscribers
        admin.topics().createNonPartitionedTopic("public/default/other")
        admin.close()
        embeddedServer = ApplicationContext.run(EmbeddedServer,
                CollectionUtils.mapOf("pulsar.service-url", pulsarContainer.pulsarBrokerUrl),
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
        def topic = "public/default/test"
        def consumerTester = context.getBean(PulsarConsumerTopicListTester)
        def producer = context.getBean(PulsarClient).newProducer().topic(topic).create()
        //simple consumer with topic list and blocking
        def message = "This should be received"
        def messageId = producer.send(message.bytes)
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

        then:
        conditions.eventually {
            message == consumerTester.latestMessage
            messageId == consumerTester.latestMessageId
        }

        cleanup:
        producer.close()
    }

    void "test defined schema pattern consumer read with async"() {
        when:
        String topic = 'persistent://public/default/other'
        PulsarConsumerTopicPatternTester consumerPatternTester = context.getBean(PulsarConsumerTopicPatternTester)
        Producer<String> producer = context.getBean(PulsarClient).newProducer(Schema.STRING).topic(topic).create()
        String message = "This should be received"
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)
        MessageId messageId = producer.send(message)

        then:
        conditions.eventually {
            message == consumerPatternTester.latestMessage
            messageId == consumerPatternTester.latestMessageId
            message == consumerPatternTester.message2
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
        @PulsarConsumer(topics = ["public/default/test"], consumerName = "single-topic-consumer")
        def topicListener(Message<byte[]> message, Consumer<byte[]> consumer) {
            latestMessageId = message.messageId
            latestMessage = new String(message.getValue())
            latestConsumer = consumer
        }
    }

    @PulsarSubscription
    static class PulsarConsumerTopicPatternTester {
        String latestMessage
        Consumer<byte[]> latestConsumer
        MessageId latestMessageId

        //Test receiving on 2 methods in same class
        String message2

        //testing default order
        @PulsarConsumer(topicsPattern = 'public/default/.*', consumerName = "consumer-async")
        def asyncTopicListener(Consumer<byte[]> consumer, Message<byte[]> message) {
            latestMessage = new String(message.getValue())
            latestConsumer = consumer
            latestMessageId = message.messageId
        }

        //testing single parameter as message body and really short autorefresh topic regex
        @PulsarConsumer(topicsPattern = 'public/default/.*', patternAutoDiscoveryPeriod = 3, consumerName = "consumer-async-simple")
        def asyncTopicListener(byte[] message) {
            message2 = new String(message)
        }
    }
}