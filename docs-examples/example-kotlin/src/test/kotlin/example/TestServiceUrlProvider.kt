package example

import io.micronaut.pulsar.annotation.PulsarServiceUrlProvider
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.ServiceUrlProvider

@PulsarServiceUrlProvider
open class TestServiceUrlProvider : ServiceUrlProvider {
    private lateinit var client: PulsarClient
    private val pulsarWrapper = PulsarWrapper()

    override fun initialize(client: PulsarClient) {
        this.client = client
    }

    override fun getServiceUrl(): String {
        return pulsarWrapper.pulsarBroker
    }

}