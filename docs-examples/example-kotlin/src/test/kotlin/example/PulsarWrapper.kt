package example

import org.testcontainers.containers.PulsarContainer
import org.testcontainers.utility.DockerImageName

class PulsarWrapper {
    val pulsar = PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:2.7.1"))

    init {
        pulsar.start()
    }

    val pulsarBroker: String get() = pulsar.pulsarBrokerUrl
}