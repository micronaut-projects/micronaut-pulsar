/*
 * Copyright 2017-2020 original authors
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
package io.micronaut.pulsar;

import io.micronaut.pulsar.annotation.PulsarProducer;
import io.micronaut.pulsar.processor.SchemaResolver;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.context.annotation.Prototype;
import io.micronaut.core.annotation.AnnotationValue;
import org.apache.pulsar.client.api.*;

@Factory
public class PulsarProducerFactory {

    @SuppressWarnings("unchecked")
    @Prototype
    public <T> Producer<T> createProducer(@Parameter PulsarClient pulsarClient,
                                          @Parameter AnnotationValue<PulsarProducer> annotationValue,
                                          @Parameter SchemaResolver schemaResolver) throws PulsarClientException {

        Class<T> bodyType = annotationValue.getRequiredValue("bodyType", Class.class);
        Schema<T> schema = (Schema<T>) schemaResolver.decideSchema(annotationValue, bodyType);

        String producerName = annotationValue.getRequiredValue("producerName", String.class);
        String topic = annotationValue.getRequiredValue("topic", String.class);

        ProducerBuilder<T> producerBuilder = pulsarClient.newProducer(schema).producerName(producerName).topic(topic);

        annotationValue.booleanValue("multiSchema").ifPresent(producerBuilder::enableMultiSchema);
        annotationValue.booleanValue("autoUpdatePartition").ifPresent(producerBuilder::autoUpdatePartitions);
        annotationValue.booleanValue("blockQueue").ifPresent(producerBuilder::blockIfQueueFull);
        annotationValue.booleanValue("batching").ifPresent(producerBuilder::blockIfQueueFull);
        annotationValue.booleanValue("chunking").ifPresent(producerBuilder::enableChunking);
        annotationValue.stringValue("encryptionKey").ifPresent(producerBuilder::addEncryptionKey);
        annotationValue.longValue("initialSequenceId").ifPresent(producerBuilder::initialSequenceId);
        annotationValue.enumValue("hashingScheme", HashingScheme.class).ifPresent(producerBuilder::hashingScheme);
        annotationValue.enumValue("compressionType", CompressionType.class)
                .ifPresent(producerBuilder::compressionType);
        annotationValue.enumValue("messageRoutingMode", MessageRoutingMode.class)
                .ifPresent(producerBuilder::messageRoutingMode);

        return producerBuilder.create();
    }
}
