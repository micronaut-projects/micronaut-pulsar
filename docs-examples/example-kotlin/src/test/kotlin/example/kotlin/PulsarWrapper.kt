package example.kotlin

import org.testcontainers.containers.PulsarContainer
import org.testcontainers.utility.DockerImageName
import java.lang.Thread.sleep

class PulsarWrapper {
    companion object {
        val pulsar = PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:2.8.0"))
        val pulsarBroker: String get() = pulsar.pulsarBrokerUrl

        init {
            pulsar.start()
            sleep(500) // give more time for startup
        }
    }
}