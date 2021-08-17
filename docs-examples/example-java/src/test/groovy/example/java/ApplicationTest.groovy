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
package example.java

import com.fasterxml.jackson.databind.ObjectMapper
import example.java.dto.PulsarMessage
import example.java.listeners.MessagingService
import example.java.listeners.ReportsTracker
import example.java.shared.SimulateEnv
import io.micronaut.pulsar.PulsarConsumerRegistry
import org.testcontainers.containers.Container
import reactor.test.StepVerifier
import spock.lang.Stepwise

import java.time.Duration
import java.time.LocalDateTime

/**
 * Simple check application boots and listens/receives messages on all ends.
 *
 * @author Haris
 * @since 1.0
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
        ReportsTracker reportsTracker = context.getBean(ReportsTracker.class)
        PulsarMessage testMessage = new PulsarMessage(LocalDateTime.now().toString(), "this is a test message")
        Container.ExecResult cmdOut = pulsar.send(mapper.writeValueAsString(testMessage))

        then:
        0 == cmdOut.exitCode
        null != consumers //when using non-async consumer subscription it should be available as soon as the context is
        null != messagingService
        !consumers.consumerIds.isEmpty()
        null != consumers.getConsumer('shared-consumer-tester')
        consumers.getConsumer('shared-consumer-tester').isConnected()
        StepVerifier.create(reportsTracker.subscribe())
                .assertNext(x -> x.contains(testMessage.message))
                .thenCancel()
                .verify(Duration.ofSeconds(10))
    }
}
