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

import io.micronaut.pulsar.annotation.PulsarProducer
import io.micronaut.pulsar.annotation.PulsarProducerClient
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import io.micronaut.core.util.StringUtils
import io.micronaut.runtime.server.EmbeddedServer
import io.reactivex.Single
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Schema
import org.testcontainers.containers.PulsarContainer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise

import java.util.concurrent.TimeUnit

@Stepwise
class PulsarProducersSpec extends Specification {

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
        embeddedServer = ApplicationContext.run(EmbeddedServer,
                CollectionUtils.mapOf("pulsar.service-url", pulsarContainer.pulsarBrokerUrl),
                StringUtils.EMPTY_STRING_ARRAY
        )
        context = embeddedServer.applicationContext
    }

    void "test simple producer"() {
        when:
        ProducerTester producer = context.getBean(ProducerTester)
        def reader = context.getBean(PulsarClient).newReader(Schema.STRING)
                .startMessageId(MessageId.latest)
                .topic("public/default/test2")
                .create()
        String message = "This should be received"
        //messages will be read sequentially
        producer.producer(message)
        String paramReturn = producer.returnOnProduce(message)
        MessageId nextMessage = producer.messageIdOnProduce(message)
        Single<MessageId> reactiveMessage = producer.reactiveMessage(message)

        then:
        message == reader.readNext(60, TimeUnit.SECONDS).value
        paramReturn == reader.readNext(60, TimeUnit.SECONDS).value
        nextMessage == reader.readNext(60, TimeUnit.SECONDS).messageId
        reactiveMessage.blockingGet() == reader.readNext(60, TimeUnit.SECONDS).messageId

        cleanup:
        reader.close()
    }

    @PulsarProducerClient
    static interface ProducerTester {
        @PulsarProducer(topic = "public/default/test2", bodyType = String, producerName = "test-producer")
        void producer(String message);
        @PulsarProducer(topic = "public/default/test2", bodyType = String, producerName = "test-producer-2")
        String returnOnProduce(String message)
        @PulsarProducer(topic = "public/default/test2", bodyType = String, producerName = "test-producer-3")
        MessageId messageIdOnProduce(String message)
        @PulsarProducer(topic = "public/default/test2", bodyType = String, producerName = "test-producer-reactive")
        Single<MessageId> reactiveMessage(String message)
    }
}
