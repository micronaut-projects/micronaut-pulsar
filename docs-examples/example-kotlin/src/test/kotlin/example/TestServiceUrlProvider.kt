package example

import io.micronaut.pulsar.annotation.PulsarServiceUrlProvider
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.ServiceUrlProvider

@PulsarServiceUrlProvider
open class TestServiceUrlProvider : ServiceUrlProvider, AutoCloseable {
    private lateinit var client: PulsarClient
    override fun initialize(client: PulsarClient) {
        this.client = client
    }

    override fun getServiceUrl(): String {
        return PulsarWrapper.pulsarBroker
    }

    override fun close() {
        PulsarWrapper.pulsar.close()
    }
}