package example

import example.dto.PulsarMessage
import example.listeners.ReportsTracker
import io.kotest.core.spec.style.StringSpec
import io.micronaut.runtime.EmbeddedApplication
import io.micronaut.test.extensions.kotest.annotation.MicronautTest
import java.time.LocalDateTime

@MicronautTest
class AppTest(private val application: EmbeddedApplication<*>) : StringSpec({
    "server running and full flow" {
        assert(application.isRunning)
        val tracker = application.applicationContext.getBean(ReportsTracker::class.java)
        val producer = application.applicationContext.getBean(TestProducer::class.java)
        val messageId = producer.produce(PulsarMessage(LocalDateTime.now().toString(), "test"))
        assert(tracker.latest()?.startsWith(messageId.toString()) ?: false)
    }
})

