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
import io.micronaut.messaging.annotation.MessageBody
import io.micronaut.pulsar.annotation.MessageProperties
import io.micronaut.pulsar.annotation.PulsarProducer
import io.micronaut.pulsar.annotation.PulsarProducerClient
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Reader
import org.apache.pulsar.client.impl.schema.StringSchema
import reactor.core.publisher.Mono
import spock.lang.Stepwise

import static java.util.concurrent.TimeUnit.SECONDS
import static org.apache.pulsar.client.api.MessageId.latest

@Stepwise
class PulsarProducersSpec extends PulsarAwareTest {

    public static final String PULSAR_PRODUCER_TEST_TOPIC = "persistent://public/default/test2"

    void "test simple producer"() {
        given:
        ProducerTester producer = context.getBean(ProducerTester)
        Reader<String> reader = context.getBean(PulsarClient)
                .newReader(new StringSchema())
                .startMessageId(latest)
                .topic(PULSAR_PRODUCER_TEST_TOPIC)
                .create()
        String message = "This should be received"

        when:
        //messages will be read sequentially
        producer.producer(message)
        String paramReturn = producer.returnOnProduce(message)
        MessageId nextMessage = producer.messageIdOnProduce(message)
        Mono<MessageId> reactiveMessage = producer.reactiveMessage(message)
        producer.withHeaders(message, ["test": "header", "other": "header2"])

        then:
        message == reader.readNext(60, SECONDS).value
        paramReturn == reader.readNext(60, SECONDS).value
        nextMessage == reader.readNext(60, SECONDS).messageId
        reactiveMessage.block() == reader.readNext(60, SECONDS).messageId
        Message<String> messageWithHeaders = reader.readNext(60, SECONDS)
        messageWithHeaders.hasProperty("test")
        messageWithHeaders.getProperty("test") == "header"

        cleanup:
        reader.close()
    }

    @Requires(property = 'spec.name', value = 'PulsarProducersSpec')
    @PulsarProducerClient
    static interface ProducerTester {

        @PulsarProducer(topic = PulsarProducersSpec.PULSAR_PRODUCER_TEST_TOPIC, producerName = "test-producer")
        void producer(String message);

        @PulsarProducer(topic = PulsarProducersSpec.PULSAR_PRODUCER_TEST_TOPIC, producerName = "test-producer-2")
        String returnOnProduce(String message)

        @PulsarProducer(topic = PulsarProducersSpec.PULSAR_PRODUCER_TEST_TOPIC, producerName = "test-producer-3")
        MessageId messageIdOnProduce(String message)

        @PulsarProducer(topic = PulsarProducersSpec.PULSAR_PRODUCER_TEST_TOPIC, producerName = "test-producer-reactive")
        Mono<MessageId> reactiveMessage(String message)

        @PulsarProducer(topic = PulsarProducersSpec.PULSAR_PRODUCER_TEST_TOPIC, producerName = "test-producer-headers")
        MessageId withHeaders(@MessageBody String message, @MessageProperties Map<String, String> properties)
    }
}
