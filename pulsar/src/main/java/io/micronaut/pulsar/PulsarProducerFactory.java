/*
 * Copyright 2021 original authors
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

/**
 * Pulsar {@link Producer} factory.
 * @author Haris Secic
 * @since 1.0
 */
@Factory
public class PulsarProducerFactory {

    /**
     * Simple factory method for producing Pulsar {@link Producer} beans.
     * @param pulsarClient main Pulsar Client bean
     * @param annotationValue method annotation to read properties from
     * @param schemaResolver schema resolver bean
     * @param <T> type of message body for pulsar producer
     * @param annotatedMethodName method name on which annotation for Pulsar Producer was set
     * @param bodyType class defining desired content type of a message passed to pulsar producer
     * @return new Pulsar producer
     * @throws PulsarClientException in case of not being able to create such Producer
     */
    @SuppressWarnings("unchecked")
    @Prototype
    public <T> Producer<T> createProducer(@Parameter PulsarClient pulsarClient,
                                          @Parameter AnnotationValue<PulsarProducer> annotationValue,
                                          @Parameter SchemaResolver schemaResolver,
                                          @Parameter String annotatedMethodName,
                                          @Parameter Class<T> bodyType) throws PulsarClientException {

        Schema<T> schema = (Schema<T>) schemaResolver.decideSchema(annotationValue, bodyType);

        String producerName = annotationValue.stringValue("producerName").orElse(annotatedMethodName);
        String topic = annotationValue.stringValue("topic").orElseGet(() -> annotationValue.getRequiredValue(String.class));

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
