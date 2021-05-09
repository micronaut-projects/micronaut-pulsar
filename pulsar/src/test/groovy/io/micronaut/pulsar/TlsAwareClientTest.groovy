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
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.SubscriptionType
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.ConcurrentLinkedDeque

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
        String tlsPathForPulsar = new File(tlsPath).path
        embeddedServer = ApplicationContext.run(EmbeddedServer.class,
                ['pulsar.service-url'       : pulsarTls.pulsarBrokerUrl,
                 'pulsar.tls-cert-file-path': tlsPathForPulsar,
                 'pulsar.tls-ciphers'       : ['TLS_RSA_WITH_AES_256_GCM_SHA384', 'TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256'],
                 'pulsar.tls-protocols'     : ['TLSv1.2', 'TLSv1.1'],
                 'spec.name'                : getClass().simpleName],
                StringUtils.EMPTY_STRING_ARRAY
        ) as EmbeddedServer
        context = embeddedServer.applicationContext
    }

    void 'test simple send receive using TLS'() {
        given:
        String test = "This is a test for TLS message"
        TlsConsumer tlsConsumer = context.getBean(TlsConsumer.class)
        TlsProducer tlsProducer = context.getBean(TlsProducer.class)

        when:
        MessageId id = tlsProducer.send(test)

        then:
        new PollingConditions(timeout: 60, delay: 1).eventually {
            test == tlsConsumer.getLastMessage()
            id.toString() == tlsConsumer.getLastMessageId()
        }
    }

    @PulsarSubscription(subscriptionName = "tlsSubscription", subscriptionType = SubscriptionType.Shared)
    static class TlsConsumer {
        private Deque<Message<String>> messages = new ConcurrentLinkedDeque<Message<String>>()

        @PulsarConsumer(topic = "public/default/test")
        void receive(Message<String> message) {
            messages.add(message)
        }

        String getLastMessage() {
            return messages.getLast().getValue()
        }

        String getLastMessageId() {
            return messages.getLast().getMessageId().toString()
        }
    }

    @PulsarProducerClient
    static interface TlsProducer {

        @PulsarProducer("public/default/test")
        MessageId send(String message)
    }
}
