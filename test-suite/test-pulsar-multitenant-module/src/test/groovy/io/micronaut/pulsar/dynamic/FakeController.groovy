/*
 * Copyright 2017-2022 original authors
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
package io.micronaut.pulsar.dynamic

import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.ApplicationEventPublisher
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Post
import io.micronaut.multitenancy.tenantresolver.TenantResolver
import io.micronaut.pulsar.events.PulsarTenantDiscoveredEvent
import org.apache.pulsar.client.api.Message

@Requires(property = 'spec.name', value = 'DynamicTenantTopicSpec')
@Controller("/")
class FakeController {
    private final ProducerDynamicTenantTopicTester producer
    private final DynamicReader dynamicReader
    final TenantResolver tenantResolver
    final ApplicationEventPublisher<PulsarTenantDiscoveredEvent> tenantPublisher

    FakeController(ProducerDynamicTenantTopicTester producer,
                   DynamicReader dynamicReader,
                   TenantResolver tenantResolver,
                   ApplicationEventPublisher<PulsarTenantDiscoveredEvent> tenantPublisher) {
        this.dynamicReader = dynamicReader
        this.producer = producer
        this.tenantResolver = tenantResolver
        this.tenantPublisher = tenantPublisher
    }

    @Post("/messages")
    String send(@Body String message) {
        return producer.send(message).toString()
    }

    @Get("/messages")
    MessageResponse next() {
        Message<String> msg = dynamicReader.read();
        return new MessageResponse(messageId: msg.messageId.toString(), message: msg.value)
    }

    @Post("/tenant")
    String addTenant(@Body String tenant) {
        tenantPublisher.publishEvent(new PulsarTenantDiscoveredEvent(tenant))
        return tenant.toString()
    }

}
