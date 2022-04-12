package io.micronaut.pulsar

import io.micronaut.context.annotation.Requires
import io.micronaut.messaging.annotation.MessageMapping
import io.micronaut.pulsar.annotation.PulsarReader
import io.micronaut.pulsar.annotation.PulsarReaderClient

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

import jakarta.inject.Singleton
import org.apache.pulsar.client.api.*
import org.apache.pulsar.client.impl.schema.StringSchema
import reactor.core.publisher.Mono
import spock.lang.Stepwise

import java.time.Duration

import static java.util.concurrent.TimeUnit.SECONDS

@Stepwise
class PulsarReaderSpec extends PulsarAwareTest {

    public static final String PULSAR_READER_TEST_TOPIC_INJECTABLE = "persistent://public/default/reader-test-injectable"
    public static final String PULSAR_READER_TEST_TOPIC_METHOD = "persistent://public/default/reader-test-method"
    public static final String PULSAR_READER_TEST_TOPIC_METHOD_WRAPPED = "persistent://public/default/reader-test-method-wrapped"
    public static final String PULSAR_READER_TEST_TOPIC_METHOD_ASYNC = "persistent://public/default/reader-test-method-async"
    public static final String PULSAR_READER_TEST_TOPIC_METHOD_ASYNC_WRAPPED = "persistent://public/default/reader-test-method-async-wrapped"

    void "test injectable reader"() {
        given:
        Producer producer = context.getBean(PulsarClient)
                .newProducer(new StringSchema())
                .topic(PulsarReaderSpec.PULSAR_READER_TEST_TOPIC_INJECTABLE)
                .producerName("string-producer")
                .create()
        Reader<String> stringDependencyReader = context.getBean(ReaderRequester).stringReader
        String message = "This is a message"

        when:
        MessageId messageId = producer.send(message)
        Message receivedMessage = stringDependencyReader.readNext(60, SECONDS)

        then:
        null != receivedMessage
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
                .topic(PulsarReaderSpec.PULSAR_READER_TEST_TOPIC_METHOD)
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
                .topic(PulsarReaderSpec.PULSAR_READER_TEST_TOPIC_METHOD_WRAPPED)
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
                .topic(PulsarReaderSpec.PULSAR_READER_TEST_TOPIC_METHOD_ASYNC)
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
                .topic(PulsarReaderSpec.PULSAR_READER_TEST_TOPIC_METHOD_ASYNC_WRAPPED)
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
        annotationValue.get().contains PULSAR_READER_TEST_TOPIC_INJECTABLE
    }

    @Singleton
    @Requires(property = 'spec.name', value = 'PulsarReaderSpec')
    static class ReaderRequester {
        private final Reader<String> stringReader

        ReaderRequester(@PulsarReader(PulsarReaderSpec.PULSAR_READER_TEST_TOPIC_INJECTABLE) Reader<String> stringReader) {
            this.stringReader = stringReader
        }
    }

    @Requires(property = 'spec.name', value = 'PulsarReaderSpec')
    @PulsarReaderClient
    static interface ReaderClientTest {
        // method consumers are created lazily so reading from latest will read null always if producer already sent
        // a message before reader existed
        @Requires(property = 'spec.name', value = 'PulsarReaderSpec')
        @PulsarReader(topic = PulsarReaderSpec.PULSAR_READER_TEST_TOPIC_METHOD, readTimeout = 60, startMessageLatest = false)
        String read();

        @Requires(property = 'spec.name', value = 'PulsarReaderSpec')
        @PulsarReader(topic = PulsarReaderSpec.PULSAR_READER_TEST_TOPIC_METHOD_WRAPPED, readTimeout = 60, startMessageLatest = false)
        Message<String> readMsg();

        @Requires(property = 'spec.name', value = 'PulsarReaderSpec')
        @PulsarReader(topic = PulsarReaderSpec.PULSAR_READER_TEST_TOPIC_METHOD_ASYNC, startMessageLatest = false)
        Mono<String> readAsync();

        @Requires(property = 'spec.name', value = 'PulsarReaderSpec')
        @PulsarReader(topic = PulsarReaderSpec.PULSAR_READER_TEST_TOPIC_METHOD_ASYNC_WRAPPED, startMessageLatest = false)
        Mono<Message<String>> readAsyncMsg();
    }
}
