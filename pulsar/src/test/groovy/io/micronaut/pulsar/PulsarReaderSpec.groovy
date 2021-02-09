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
import io.micronaut.pulsar.annotation.PulsarReader
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Reader
import spock.lang.Stepwise

import javax.inject.Singleton

import static java.util.concurrent.TimeUnit.SECONDS
import static org.apache.pulsar.client.api.Schema.STRING

@Stepwise
class PulsarReaderSpec extends PulsarAwareTest {

    private static final String PULSAR_READER_TEST_TOPIC = "public/default/simple-reader"

    void setupSpec() {
        PulsarDefaultContainer.createNonPartitionedTopic(PULSAR_READER_TEST_TOPIC)
    }

    @Singleton
    @Requires(property = 'spec.name', value = 'PulsarReaderSpec')
    static class ReaderRequester {
        private final Reader<String> stringReader

        ReaderRequester(@PulsarReader(PulsarReaderSpec.PULSAR_READER_TEST_TOPIC) Reader<String> stringReader) {
            this.stringReader = stringReader
        }
    }

    void "test simple reader"() {
        given:
        String topic = "persistent://$PULSAR_READER_TEST_TOPIC"
        Producer producer = context.getBean(PulsarClient)
                .newProducer(STRING)
                .topic(topic)
                .producerName("string-producer")
                .create()
        Reader stringReader = context.getBean(ReaderRequester).stringReader
        String message = "This is a message"
        MessageId messageId = producer.send(message)

        when:
        Message receivedMessage = stringReader.readNext(60, SECONDS)

        then:
        messageId == receivedMessage.messageId
        message == receivedMessage.value

        cleanup:
        stringReader.close()
        producer.close()
    }
}
