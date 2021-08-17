package kotlinexample

import org.testcontainers.containers.PulsarContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.utility.DockerImageName

class PulsarWrapper {
    companion object {
        @Container
        @JvmStatic
        lateinit var pulsar: PulsarContainer
        @JvmStatic
        val pulsarBroker: String get() = pulsar.pulsarBrokerUrl

        @JvmStatic
        fun start() {
            if (Companion::pulsar.isInitialized) {
                return
            }
            pulsar = PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:2.8.0"))
            pulsar.start()
            pulsar.execInContainer("/bin/bash", "-c", "/pulsar/bin/pulsar-admin namespaces set-is-allow-auto-update-schema --enable public/default")
            val result = pulsar.execInContainer("/bin/bash", "-c", "/pulsar/bin/pulsar-admin topics create persistent://public/default/messages-kotlin-docs")
            if (0 != result.exitCode) throw Exception(result.stderr ?: result.stdout)
        }

        @JvmStatic
        fun isRunning() = pulsar.isRunning

        @JvmStatic
        fun stop() {
            if (Companion::pulsar.isInitialized) {
                pulsar.stop()
            }
        }
    }
}