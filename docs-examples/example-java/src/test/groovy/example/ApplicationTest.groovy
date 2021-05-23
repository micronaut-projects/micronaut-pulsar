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
import example.listeners.MessagingService
import example.listeners.ReportsTracker
import example.shared.SimulateEnv
import io.micronaut.pulsar.PulsarConsumerRegistry
import io.reactivex.subscribers.TestSubscriber
import org.testcontainers.containers.Container
import spock.lang.Stepwise
import spock.util.concurrent.PollingConditions

import java.time.LocalDateTime

/**
 * Simple check application boots nad listens/receives messages on all ends.
 *
 * @author Haris* @since 1.0
 */
@Stepwise
class ApplicationTest extends SimulateEnv {

    void 'should start containers and subscribe micronaut app to them'() {
        expect:
        context.running

        when:
        MessagingService messagingService = context.getBean(MessagingService.class)
        PulsarConsumerRegistry consumers = context.getBean(PulsarConsumerRegistry.class)

        then:
        null != messagingService
        null != consumers
        !consumers.consumerIds.isEmpty()
        null != consumers.getConsumer('shared-consumer-tester')
    }

    void 'should receive a message on a report topic'() {
        expect:
        context.isRunning()

        when:
        ObjectMapper mapper = context.getBean(ObjectMapper.class)
        MessagingService messagingService = context.getBean(MessagingService.class)
        PulsarConsumerRegistry consumers = context.getBean(PulsarConsumerRegistry.class)
        TestSubscriber<String> reportsTracker = context.getBean(ReportsTracker.class).subscribe().test()
        PulsarMessage testMessage = new PulsarMessage(LocalDateTime.now().toString(), "this is a test message")
        Container.ExecResult cmdOut = pulsar.send(mapper.writeValueAsString(testMessage))

        then:
        0 == cmdOut.exitCode
        null != consumers //when using non-async consumer subscription it should be available as soon as the context is
        null != messagingService
        !consumers.consumerIds.isEmpty()
        null != consumers.getConsumer('shared-consumer-tester')
        consumers.getConsumer('shared-consumer-tester').isConnected()
        reportsTracker.awaitCount(1)
        reportsTracker.values().any { it.contains(testMessage.message) }

        cleanup:
        reportsTracker.dispose()
    }
}
