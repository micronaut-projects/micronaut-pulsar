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
package example

import com.fasterxml.jackson.databind.ObjectMapper
import example.dto.PulsarMessage
import example.listeners.ReportsTracker
import example.shared.SimulateEnv
import spock.lang.Stepwise

import java.time.LocalDateTime

/**
 * Simple check application boots nad listens/receives messages on all ends.
 *
 * @author Haris
 * @since 1.0
 */
@Stepwise
class ApplicationTest extends SimulateEnv {

    void 'should start containers and subscribe micronaut app them'() {
        expect:
        context.running
    }

    void 'should receive a message on a report topic'() {
        given:
        ReportsTracker reportsTracker = context.getBean(ReportsTracker.class)
        ObjectMapper mapper = context.getBean(ObjectMapper.class)
        def testMessage = new PulsarMessage(LocalDateTime.now().toString(), "this is a test message")
        def subscription = reportsTracker.subscribe()
        subscription.subscribe()
        pulsar.send(mapper.writeValueAsString(testMessage))

        expect:
        def list = subscription.blockingFirst()
        null != list
        list.contains("this is a test message")
    }
}
