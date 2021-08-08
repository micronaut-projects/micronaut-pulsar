/*
 * Copyright 2017-2021 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package example.java.web;

import example.java.listeners.ReportsTracker;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import reactor.core.publisher.Flux;

/**
 * TODO javadoc.
 */
@Controller("reports")
public class ReportResource {

    private final ReportsTracker reportsTracker;

    public ReportResource(ReportsTracker reportsTracker) {
        this.reportsTracker = reportsTracker;
    }

    /**
     * @return SSE stream of newest pulsar messages that keeps alive until client close or server drop
     */
    @Get(produces = MediaType.TEXT_EVENT_STREAM)
    public Flux<String> latestReport() {
        return reportsTracker.subscribe();
    }
}
