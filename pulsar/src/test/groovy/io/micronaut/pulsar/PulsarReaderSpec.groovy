package io.micronaut.pulsar

import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import io.micronaut.core.util.StringUtils
import io.micronaut.inject.FieldInjectionPoint
import io.micronaut.pulsar.annotation.PulsarReader
import io.micronaut.runtime.server.EmbeddedServer
import io.reactivex.Single
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Reader
import org.apache.pulsar.client.api.Schema
import org.testcontainers.containers.PulsarContainer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise

import javax.inject.Singleton
import java.util.concurrent.TimeUnit

@Stepwise
class PulsarReaderSpec extends Specification {

    @AutoCleanup
    @Shared
    PulsarContainer pulsarContainer = new PulsarContainer("2.6.1").with {
        it.start()
        it
    }

    @Shared
    @AutoCleanup
    ApplicationContext context

    @Shared
    @AutoCleanup
    EmbeddedServer embeddedServer

    def setupSpec() {
        embeddedServer = ApplicationContext.run(EmbeddedServer,
                CollectionUtils.mapOf("pulsar.service-url", pulsarContainer.pulsarBrokerUrl),
                StringUtils.EMPTY_STRING_ARRAY
        )
        context = embeddedServer.applicationContext
    }

    @Singleton
    static class ReaderRequester {

        private Reader<String> stringReader

        ReaderRequester(@PulsarReader("public/default/test") Reader<String> stringReader) {
            this.stringReader = stringReader
        }
    }

    void "test simple reader"() {
        given:
        def topic = "persistent://public/default/test"
        def producer = context.getBean(PulsarClient).newProducer(Schema.STRING).topic(topic)
                .producerName("string-producer").create()
        def stringReader = context.getBean(ReaderRequester.class).stringReader
        def message = "This is a message"
        def messageId = producer.send(message)

        when:
        def receivedMessage = stringReader.readNext(60, TimeUnit.SECONDS)

        then:
        messageId == receivedMessage.messageId
        message == receivedMessage.value

        cleanup:
        stringReader.close()
        producer.close()
    }
}
