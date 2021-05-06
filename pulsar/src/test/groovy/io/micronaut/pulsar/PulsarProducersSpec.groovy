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
import io.micronaut.pulsar.annotation.PulsarProducer
import io.micronaut.pulsar.annotation.PulsarProducerClient
import io.micronaut.pulsar.shared.PulsarAwareTest
import io.reactivex.Single
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Reader
import org.apache.pulsar.client.impl.schema.StringSchema
import spock.lang.Stepwise

import static java.util.concurrent.TimeUnit.SECONDS
import static org.apache.pulsar.client.api.MessageId.latest

@Stepwise
class PulsarProducersSpec extends PulsarAwareTest {

    private static final String PULSAR_PRODUCER_TEST_TOPIC = "persistent://public/default/test2"

    static {
        PulsarDefaultContainer.createNonPartitionedTopic(PULSAR_PRODUCER_TEST_TOPIC)
    }

    void "test simple producer"() {
        when:
        ProducerTester producer = context.getBean(ProducerTester)
        Reader reader = context.getBean(PulsarClient)
                .newReader(new StringSchema())
                .startMessageId(latest)
                .topic(PULSAR_PRODUCER_TEST_TOPIC)
                .create()
        String message = "This should be received"
        //messages will be read sequentially
        producer.producer(message)
        String paramReturn = producer.returnOnProduce(message)
        MessageId nextMessage = producer.messageIdOnProduce(message)
        Single<MessageId> reactiveMessage = producer.reactiveMessage(message)

        then:
        message == reader.readNext(60, SECONDS).value
        paramReturn == reader.readNext(60, SECONDS).value
        nextMessage == reader.readNext(60, SECONDS).messageId
        reactiveMessage.blockingGet() == reader.readNext(60, SECONDS).messageId

        cleanup:
        reader.close()
    }

    @Requires(property = 'spec.name', value = 'PulsarProducersSpec')
    @PulsarProducerClient
    static interface ProducerTester {

        @PulsarProducer(
                topic = PulsarProducersSpec.PULSAR_PRODUCER_TEST_TOPIC,
                producerName = "test-producer")
        void producer(String message);

        @PulsarProducer(
                topic = PulsarProducersSpec.PULSAR_PRODUCER_TEST_TOPIC,
                producerName = "test-producer-2")
        String returnOnProduce(String message)

        @PulsarProducer(
                topic = PulsarProducersSpec.PULSAR_PRODUCER_TEST_TOPIC,
                producerName = "test-producer-3")
        MessageId messageIdOnProduce(String message)

        @PulsarProducer(
                topic = PulsarProducersSpec.PULSAR_PRODUCER_TEST_TOPIC,
                producerName = "test-producer-reactive")
        Single<MessageId> reactiveMessage(String message)
    }
}
