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
package example.web;

import example.listeners.ReportsTracker;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.reactivex.Flowable;

/**
 * Reports "streaming" endpoint.
 *
 * @author Haris
 * @since 1.0
 */
@Controller("reports")
public final class ReportResource {

    private final ReportsTracker reportsTracker;

    public ReportResource(ReportsTracker reportsTracker) {
        this.reportsTracker = reportsTracker;
    }

    @Get(produces = MediaType.TEXT_EVENT_STREAM)
    public Flowable<String> latestReport() {
        return reportsTracker.subscribe();
    }
}
