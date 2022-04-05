package io.micronaut.pulsar
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


import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import io.micronaut.pulsar.annotation.PulsarConsumer
import io.micronaut.pulsar.annotation.PulsarProducer
import io.micronaut.pulsar.annotation.PulsarProducerClient
import io.micronaut.pulsar.annotation.PulsarSubscription
import io.micronaut.pulsar.shared.PulsarTls
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Reader
import org.apache.pulsar.client.impl.schema.StringSchema
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise
import spock.util.concurrent.BlockingVariables

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

@Stepwise
class TlsAwareClientTest extends Specification {

    @Shared
    @AutoCleanup
    ApplicationContext context

    void setupSpec() {
        String tlsPath = ClassLoader.getSystemClassLoader().getResource('ca.cert.pem').path
        String tlsPathForPulsar = new File(tlsPath).absolutePath
        this.context = ApplicationContext.run(
                ['pulsar.service-url'                 : PulsarTls.pulsarBrokerTlsUrl,
                 'pulsar.tls-cert-file-path'          : tlsPathForPulsar,
                 'pulsar.shutdown-on-subscriber-error': true,
                 'pulsar.tls-ciphers'                 : ['TLS_RSA_WITH_AES_256_GCM_SHA384', 'TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256'],
                 'pulsar.tls-protocols'               : ['TLSv1.3', 'TLSv1.2', 'TLSv1.1'],
                 'spec.name'                          : getClass().simpleName],
                Environment.TEST
        )
    }

    void 'test simple send receive using TLS'() {
        given:
        String test = "This is a test for TLS message"
        TlsProducer tlsProducer = context.getBean(TlsProducer.class)
        TlsConsumer tlsConsumer = context.getBean(TlsConsumer.class)
        Reader<String> blockingReader = context.getBean(PulsarClient)
                .newReader(new StringSchema())
                .readerName("blocking-tls-reader")
                .topic("persistent://public/default/test-tls")
                .startMessageId(MessageId.latest)
                .startMessageIdInclusive()
                .create()
        tlsConsumer.blocking = new BlockingVariables(120)
        Consumer<String> consumer = context.getBean(PulsarConsumerRegistry.class).getConsumer("tls-receiver")

        expect:
        consumer.isConnected()

        when:
        MessageId id = tlsProducer.send(test)

        then:
        Message<String> block = blockingReader.readNext(1, TimeUnit.MINUTES)
        id == block.messageId
        test == block.value
        id == tlsConsumer.blocking.getProperty("id")
        test == tlsConsumer.blocking.getProperty("value")

        cleanup:
        blockingReader.closeAsync()
    }

    @Requires(property = 'spec.name', value = 'TlsAwareClientTest')
    @PulsarSubscription(subscriptionName = "tlsSubscription")
    static class TlsConsumer {
        private Message<String> latest
        private BlockingVariables blocking
        private final ReadWriteLock lock = new ReentrantReadWriteLock()

        @PulsarConsumer(topic = "persistent://public/default/test-tls", consumerName = "tls-receiver", subscribeAsync = false)
        void receive(Message<String> message) {
            if (null != latest) return
            lock.writeLock().lock()
            latest = message
            if (blocking != null) {
                blocking.setProperty("id", latest.messageId)
                blocking.setProperty("value", latest.value)
            }
            lock.writeLock().unlock()
        }

        void setBlocking(BlockingVariables blocking) {
            this.blocking = blocking
            if (null != latest) {
                this.blocking.setProperty("id", latest.messageId)
                this.blocking.setProperty("value", latest.value)
            }
        }

        BlockingVariables getBlocking() {
            return blocking
        }
    }

    @Requires(property = 'spec.name', value = 'TlsAwareClientTest')
    @PulsarProducerClient
    static interface TlsProducer {

        @PulsarProducer(topic = "persistent://public/default/test-tls")
        MessageId send(String message)
    }
}
