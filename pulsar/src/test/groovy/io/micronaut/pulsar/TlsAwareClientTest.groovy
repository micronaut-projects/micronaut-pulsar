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
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Reader
import org.apache.pulsar.client.impl.schema.StringSchema
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.TimeUnit

import static org.apache.pulsar.client.api.MessageId.latest

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
        embeddedServer = ApplicationContext.run(EmbeddedServer.class,
                ['pulsar.service-url': pulsarTls.getBrokerUrl(),
                 'spec.name'         : getClass().simpleName],
                StringUtils.EMPTY_STRING_ARRAY
        ) as EmbeddedServer
        context = embeddedServer.applicationContext
    }

    void 'test simple send receive using TLS'() {
        given:
        String test = "This is a test for TLS message"
        TlsConsumer tlsConsumer = context.getBean(TlsConsumer.class)
        TlsProducer tlsProducer = context.getBean(TlsProducer.class)
        Reader reader = context.getBean(PulsarClient)
                .newReader(new StringSchema())
                .startMessageId(latest)
                .topic("public/default/test")
                .create()

        when:
        tlsProducer.send(test)

        then:
        test == reader.readNext(60, TimeUnit.SECONDS).value
        new PollingConditions(timeout: 60, delay: 1).eventually {
            test == tlsConsumer.lastMessage
        }
    }

    @PulsarSubscription(subscriptionName = "tlsSubscription")
    class TlsConsumer {
        private Deque<String> messages = new ConcurrentLinkedDeque<String>()

        @PulsarConsumer(topic = "public/default/test")
        void receive(String message) {
            messages.add(message)
        }

        String getLastMessage() {
            return messages.getLast();
        }
    }

    @PulsarProducerClient
    interface TlsProducer {

        @PulsarProducer("public/default/test")
        void send(String message)
    }
}
