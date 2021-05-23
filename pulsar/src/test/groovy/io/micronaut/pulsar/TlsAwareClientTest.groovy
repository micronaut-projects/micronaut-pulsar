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

import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.StringUtils
import io.micronaut.pulsar.annotation.PulsarConsumer
import io.micronaut.pulsar.annotation.PulsarProducer
import io.micronaut.pulsar.annotation.PulsarProducerClient
import io.micronaut.pulsar.annotation.PulsarSubscription
import io.micronaut.pulsar.shared.PulsarTls
import io.micronaut.runtime.server.EmbeddedServer
import org.apache.pulsar.client.api.*
import org.apache.pulsar.client.impl.schema.StringSchema
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise
import spock.util.concurrent.PollingConditions

import java.util.concurrent.TimeUnit

@Stepwise
class TlsAwareClientTest extends Specification {

    @Shared
    @AutoCleanup
    PulsarTls pulsarTls

    @Shared
    @AutoCleanup
    ApplicationContext context

    @Shared
    @AutoCleanup
    EmbeddedServer embeddedServer

    void setupSpec() {
        pulsarTls = new PulsarTls()
        pulsarTls.start()
        String tlsPath = ClassLoader.getSystemClassLoader().getResource('ca.cert.pem').path
        String tlsPathForPulsar = new File(tlsPath).absolutePath
        embeddedServer = ApplicationContext.run(EmbeddedServer.class,
                ['pulsar.service-url'                     : pulsarTls.pulsarBrokerUrl,
                 'pulsar.tls-cert-file-path'              : tlsPathForPulsar,
                 'pulsar.shutdown-on-subscription-failure': true,
                 'pulsar.tls-ciphers'                     : ['TLS_RSA_WITH_AES_256_GCM_SHA384', 'TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256'],
                 'pulsar.tls-protocols'                   : ['TLSv1.2', 'TLSv1.1'],
                 'spec.name'                              : getClass().simpleName],
                StringUtils.EMPTY_STRING_ARRAY
        ) as EmbeddedServer
        context = embeddedServer.applicationContext
    }

    void 'test simple send receive using TLS'() {
        given:
        String test = "This is a test for TLS message"
        TlsProducer tlsProducer = context.getBean(TlsProducer.class)
        TlsConsumer tlsConsumer = context.getBean(TlsConsumer.class)
        Reader<String> blockingReader = context.getBean(PulsarClient.class)
                .newReader(new StringSchema())
                .readerName("blocking-tls-reader")
                .topic("persistent://public/default/test")
                .startMessageId(MessageId.latest)
                .startMessageIdInclusive()
                .create()

        when:
        MessageId id = tlsProducer.send(test)

        then:
        Message<String> block = blockingReader.readNext(1, TimeUnit.MINUTES)
        id == block.messageId
        test == block.value
        new PollingConditions(timeout: 80, delay: 2, initialDelay: 1).eventually {
            test == tlsConsumer.getLastMessage()
            id.toString() == tlsConsumer.getLastMessageId()
        }
    }

    @PulsarSubscription(subscriptionName = "tlsSubscription", subscriptionType = SubscriptionType.Shared)
    static class TlsConsumer {
        private Deque<Message<String>> messages = new ArrayDeque<>()

        @PulsarConsumer(topic = "persistent://public/default/test",
                subscribeAsync = false,
                consumerName = "tls-receiver"
        )
        void receive(Message<String> message) {
            messages.add(message)
        }

        String getLastMessage() {
            return messages.peekLast()?.getValue()
        }

        String getLastMessageId() {
            return messages.peekLast()?.getMessageId()?.toString()
        }
    }

    @PulsarProducerClient
    static interface TlsProducer {

        @PulsarProducer(topic = "persistent://public/default/test")
        MessageId send(String message)
    }

    void cleanupSpec() {
        embeddedServer.stop()
    }
}
