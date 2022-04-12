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
package io.micronaut.pulsar;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.context.annotation.Prototype;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.type.Argument;
import io.micronaut.messaging.exceptions.MessagingClientException;
import io.micronaut.pulsar.annotation.PulsarProducer;
import io.micronaut.pulsar.config.PulsarClientConfiguration;
import io.micronaut.pulsar.processor.DefaultSchemaHandler;
import io.micronaut.pulsar.processor.PulsarArgumentHandler;
import io.micronaut.pulsar.processor.TopicResolver;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;

/**
 * Pulsar {@link Producer} factory.
 *
 * @author Haris Secic
 * @since 1.0
 */
@Factory
public class PulsarProducerFactory {

    private final PulsarClientConfiguration configuration;
    private final TopicResolver topicResolver;

    public PulsarProducerFactory(final PulsarClientConfiguration configuration, final TopicResolver topicResolver) {
        this.configuration = configuration;
        this.topicResolver = topicResolver;
    }

    /**
     * Simple factory method for producing Pulsar {@link Producer} beans.
     *
     * @param pulsarClient         main Pulsar Client bean
     * @param annotationValue      method annotation to read properties from
     * @param methodArguments      arguments passed to method annotated with @PulsarProducer
     * @param simpleSchemaResolver schema resolver bean
     * @param <T>                  type of message body for pulsar producer
     * @param annotatedMethodName  method name on which annotation for Pulsar Producer was set
     * @return new Pulsar producer
     */
    @SuppressWarnings("unchecked")
    @Prototype
    public <T> Producer<T> createProducer(@Parameter PulsarClient pulsarClient,
                                          @Parameter AnnotationValue<PulsarProducer> annotationValue,
                                          @Parameter Argument<?>[] methodArguments,
                                          @Parameter DefaultSchemaHandler simpleSchemaResolver,
                                          @Parameter String annotatedMethodName) throws MessagingClientException {

        final PulsarArgumentHandler argsHandler = new PulsarArgumentHandler(methodArguments, annotatedMethodName);
        final Schema<T> schema = (Schema<T>) simpleSchemaResolver.decideSchema(argsHandler.getBodyArgument(),
            argsHandler.getKeyArgument(),
            annotationValue,
            annotatedMethodName);

        final String producerName = annotationValue.stringValue("producerName").orElse(annotatedMethodName);
        final String topic = annotationValue.stringValue("topic", null)
            .orElseGet(() -> annotationValue.stringValue("value", null).orElse(null));
        if (null == topic) {
            if (configuration.getShutdownOnSubscriberError()) {
                throw new Error("Failed to instantiate Pulsar producer " + producerName + " due to missing topic");
            }
            throw new MessagingClientException("Topic value missing for producer " + producerName);
        }

        final ProducerBuilder<T> producerBuilder = new ProducerBuilderImpl<>((PulsarClientImpl) pulsarClient, schema)
            .producerName(producerName)
            .topic(topicResolver.resolve(topic));

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

        try {
            return producerBuilder.create();
        } catch (Exception ex) {
            final String message = String.format("Failed to initialize Pulsar producer %s on topic %s", producerName, topic);
            throw new MessagingClientException(message, ex);
        }
    }
}
