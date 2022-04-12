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
import io.micronaut.pulsar.DynamicTenantTopicSpec
import io.micronaut.pulsar.annotation.PulsarProducer
import io.micronaut.pulsar.annotation.PulsarProducerClient
import jakarta.inject.Singleton
import org.apache.pulsar.client.api.MessageId

@Requires(property = 'spec.name', value = 'DynamicTenantTopicSpec')
@PulsarProducerClient
interface ProducerDynamicTenantTopicTester {

    @PulsarProducer(
            topic = DynamicTenantTopicSpec.PULSAR_DYNAMIC_TENANT_TEST_TOPIC,
            producerName = 'dynamic-topic-producer')
    MessageId send(String message)
}
