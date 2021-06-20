package example

import org.testcontainers.containers.PulsarContainer
import org.testcontainers.utility.DockerImageName
import java.lang.Thread.sleep

object PulsarWrapper {
    val pulsar = PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:2.8.0"))

    init {
        pulsar.start()
        sleep(1000) // give more time for startup
    }

    val pulsarBroker: String get() = pulsar.pulsarBrokerUrl
}