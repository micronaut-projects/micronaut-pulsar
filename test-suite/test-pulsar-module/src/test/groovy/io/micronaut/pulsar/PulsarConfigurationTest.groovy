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
package io.micronaut.pulsar

import io.micronaut.protobuf.codec.ProtobufferCodec
import io.micronaut.pulsar.config.PulsarClientConfiguration
import io.micronaut.pulsar.intercept.PulsarProducerAdvice
import io.micronaut.pulsar.intercept.PulsarReaderAdvice
import io.micronaut.pulsar.processor.PulsarConsumerProcessor
import io.micronaut.pulsar.schemas.avro.AvroSchemaResolver
import io.micronaut.pulsar.schemas.json.JsonSchemaResolver
import io.micronaut.pulsar.schemas.protobuf.ProtobufSchemaResolver
import io.micronaut.pulsar.shared.PulsarTls
import org.apache.pulsar.client.api.PulsarClient
import spock.lang.Stepwise

@Stepwise
class PulsarConfigurationTest extends PulsarAwareTest {

    void "test load configuration"() {
        expect:
        context.isRunning()
        context.containsBean(PulsarClientConfiguration)
        context.containsBean(PulsarClient)
        context.containsBean(PulsarConsumerProcessor)
        context.containsBean(PulsarProducerAdvice)
        context.containsBean(PulsarReaderAdvice)
        context.containsBean(JsonSchemaResolver.class)
        context.containsBean(AvroSchemaResolver.class)
        context.containsBean(ProtobufferCodec.class)
        context.containsBean(ProtobufSchemaResolver.class)
        PulsarTls.pulsarBrokerUrl == context.getBean(PulsarClientConfiguration).serviceUrl
    }
}
