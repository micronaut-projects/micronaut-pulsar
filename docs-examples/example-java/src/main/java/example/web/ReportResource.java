package example.web;

import example.listeners.ReportsTracker;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.reactivex.Flowable;

@Controller("reports")
public class ReportResource {

    private final ReportsTracker reportsTracker;

    public ReportResource(ReportsTracker reportsTracker) {
        this.reportsTracker = reportsTracker;
    }

    @Get(produces = MediaType.APPLICATION_JSON_STREAM)
    public Flowable<String> latestReport() {
        return reportsTracker.subscribe();
    }
}
