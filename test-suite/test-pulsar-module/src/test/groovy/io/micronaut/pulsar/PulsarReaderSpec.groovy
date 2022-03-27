package io.micronaut.pulsar
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


import io.micronaut.context.annotation.Requires
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.messaging.annotation.MessageMapping
import io.micronaut.pulsar.annotation.PulsarReader
import io.micronaut.pulsar.annotation.PulsarReaderClient
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Reader
import org.apache.pulsar.client.impl.schema.StringSchema
import reactor.core.publisher.Mono
import spock.lang.Stepwise

import java.time.Duration

import static java.util.concurrent.TimeUnit.SECONDS

@Stepwise
abstract class PulsarReaderSpec extends PulsarAwareTest {

    public static final String PULSAR_READER_TEST_TOPIC = "persistent://public/default/simple-reader"

    void "test injectable reader"() {
        given:
        Producer producer = context.getBean(PulsarClient)
                .newProducer(new StringSchema())
                .topic(PULSAR_READER_TEST_TOPIC)
                .producerName("string-producer")
                .create()
        Reader<String> stringDependencyReader = context.getBean(ReaderRequester).stringReader
        String message = "This is a message"

        when:
        MessageId messageId = producer.send(message)
        Message receivedMessage = stringDependencyReader.readNext(60, SECONDS)

        then:
        messageId == receivedMessage.messageId
        message == receivedMessage.value

        cleanup:
        stringDependencyReader.close()
        producer.close()
    }

    void "test abstract method reader"() {
        given:
        Producer producer = context.getBean(PulsarClient)
                .newProducer(new StringSchema())
                .topic(PULSAR_READER_TEST_TOPIC)
                .producerName("string-producer")
                .create()
        ReaderClientTest readerClientTest = context.getBean(ReaderClientTest)
        String message = "This is a message"

        when:
        producer.send(message)
        String receivedString = readerClientTest.read()

        then:
        message == receivedString

        cleanup:
        producer.close()
    }

    void "test abstract method reader wrapped Message"() {
        given:
        Producer producer = context.getBean(PulsarClient)
                .newProducer(new StringSchema())
                .topic(PULSAR_READER_TEST_TOPIC)
                .producerName("string-producer")
                .create()
        ReaderClientTest readerClientTest = context.getBean(ReaderClientTest)
        String message = "This is a message"

        when:
        MessageId messageId = producer.send(message)
        Message<String> receivedMessage = readerClientTest.readMsg()

        then:
        messageId == receivedMessage.messageId
        message == receivedMessage.value

        cleanup:
        producer.close()
    }

    void "test abstract method async reader"() {
        given:
        Producer producer = context.getBean(PulsarClient)
                .newProducer(new StringSchema())
                .topic(PULSAR_READER_TEST_TOPIC)
                .producerName("string-producer")
                .create()
        ReaderClientTest readerClientTest = context.getBean(ReaderClientTest)
        String message = "This is a message"

        when:
        producer.send(message)
        String receivedString = readerClientTest.readAsync().block(Duration.ofSeconds(60))

        then:
        message == receivedString

        cleanup:
        producer.close()
    }

    void "test abstract method async reader wrapped Message"() {
        given:
        Producer producer = context.getBean(PulsarClient)
                .newProducer(new StringSchema())
                .topic(PULSAR_READER_TEST_TOPIC)
                .producerName("string-producer")
                .create()
        ReaderClientTest readerClientTest = context.getBean(ReaderClientTest)
        String message = "This is a message"

        when:
        MessageId messageId = producer.send(message)
        Message<String> receivedMessage = readerClientTest.readAsyncMsg().block(Duration.ofSeconds(60))

        then:
        messageId == receivedMessage.messageId
        message == receivedMessage.value

        cleanup:
        producer.close()
    }

    void "test reader has MessageMapping annotation with expected topic value"() {
        given:
        def definition = context.getBeanDefinition(ReaderRequester)

        when:
        def constructor = definition.constructor
        def annotationValue = constructor.arguments.first().annotationMetadata.getValue(MessageMapping, String[])

        then:
        annotationValue.isPresent()
        annotationValue.get().contains PULSAR_READER_TEST_TOPIC
    }

    @Singleton
    @Requires(property = 'spec.name', value = 'PulsarReaderSpec')
    static class ReaderRequester {
        private final Reader<String> stringReader

        ReaderRequester(@PulsarReader(PulsarReaderSpec.PULSAR_READER_TEST_TOPIC) Reader<String> stringReader) {
            this.stringReader = stringReader
        }
    }

    @Requires(property = 'spec.name', value = 'PulsarReaderSpec')
    @PulsarReaderClient
    static interface ReaderClientTest {
        @Requires(property = 'spec.name', value = 'PulsarReaderSpec')
        @PulsarReader(topic = PulsarReaderSpec.PULSAR_READER_TEST_TOPIC, readTimeout = 60)
        abstract String read();

        @Requires(property = 'spec.name', value = 'PulsarReaderSpec')
        @PulsarReader(topic = PulsarReaderSpec.PULSAR_READER_TEST_TOPIC, readTimeout = 60)
        abstract Message<String> readMsg();

        @Requires(property = 'spec.name', value = 'PulsarReaderSpec')
        @PulsarReader(topic = PulsarReaderSpec.PULSAR_READER_TEST_TOPIC)
        abstract Mono<String> readAsync();

        @Requires(property = 'spec.name', value = 'PulsarReaderSpec')
        @PulsarReader(topic = PulsarReaderSpec.PULSAR_READER_TEST_TOPIC)
        abstract Mono<Message<String>> readAsyncMsg();
    }
}
