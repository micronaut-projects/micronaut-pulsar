package example.web

import example.listeners.ReportsTracker
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get

@Controller("reports")
class ReportsResource(private val reports: ReportsTracker) {

    @Get
    suspend fun reports() = reports.latest()
}