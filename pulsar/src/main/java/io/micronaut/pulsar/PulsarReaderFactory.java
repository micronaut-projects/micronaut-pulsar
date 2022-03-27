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
package io.micronaut.pulsar;

import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.context.annotation.Prototype;
import io.micronaut.context.exceptions.ConfigurationException;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.type.Argument;
import io.micronaut.inject.ArgumentInjectionPoint;
import io.micronaut.inject.ConstructorInjectionPoint;
import io.micronaut.inject.FieldInjectionPoint;
import io.micronaut.inject.InjectionPoint;
import io.micronaut.pulsar.annotation.PulsarReader;
import io.micronaut.pulsar.processor.DefaultSchemaHandler;
import io.micronaut.pulsar.processor.TopicResolver;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.schema.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Create pulsar reader beans for fields annotated with PulsarReader.
 *
 * @author Haris Secic
 * @since 1.0
 */
@Factory
public class PulsarReaderFactory implements AutoCloseable, PulsarReaderRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarReaderFactory.class);

    private final Map<String, Reader<?>> readers = new ConcurrentHashMap<>();
    private final PulsarClient pulsarClient;
    private final DefaultSchemaHandler simpleSchemaResolver;
    private final TopicResolver topicResolver;

    public PulsarReaderFactory(final PulsarClient pulsarClient,
                               final DefaultSchemaHandler simpleSchemaResolver,
                               final TopicResolver topicResolver) {
        this.pulsarClient = pulsarClient;
        this.simpleSchemaResolver = simpleSchemaResolver;
        this.topicResolver = topicResolver;
    }

    /**
     * Create Pulsar Reader for given injection point if missing.
     *
     * @param injectionPoint field or argument injection point
     * @return new instance of Pulsar reader if missing; otherwise return from cache
     * @throws PulsarClientException in case of not being able to create such Reader
     */
    @Prototype
    public Reader<?> createReader(final InjectionPoint<Reader<?>> injectionPoint) throws PulsarClientException {

        final AnnotationValue<PulsarReader> annotation = injectionPoint.getAnnotation(PulsarReader.class);
        if (null == annotation) {
            throw new IllegalStateException("Failed to get value for bean annotated with PulsarReader");
        }

        final String topicValue = annotation.getRequiredValue(String.class);
        final Argument<?> readerArgument;
        final String declaredName;
        final String target;

        if (injectionPoint instanceof ArgumentInjectionPoint) {
            final ArgumentInjectionPoint<?, Reader<?>> argumentInjection = (ArgumentInjectionPoint<?, Reader<?>>) injectionPoint;
            readerArgument = argumentInjection.getArgument().getFirstTypeVariable()
                .orElse(Argument.of(byte[].class));
            declaredName = argumentInjection.getArgument().getName();
            target = argumentInjection.getDeclaringBean().getName() + " " + declaredName;
            if (argumentInjection.getOuterInjectionPoint() instanceof ConstructorInjectionPoint
                && TopicResolver.isDynamicTenantInTopic(topicValue)) {
                throw new ConfigurationException(String.format(
                    "Cannot use dynamic tenant in topics for constructor injected Readers in %s",
                    target
                ));
            }
        } else if (injectionPoint instanceof FieldInjectionPoint) {
            final FieldInjectionPoint<?, Reader<?>> fieldInjection = (FieldInjectionPoint<?, Reader<?>>) injectionPoint;
            readerArgument = fieldInjection.asArgument().getFirstTypeVariable()
                .orElse(Argument.of(byte[].class));
            declaredName = fieldInjection.getName();
            target = fieldInjection.getDeclaringBean().getName() + "::" + declaredName;
            if (TopicResolver.isDynamicTenantInTopic(topicValue)) {
                throw new ConfigurationException(String.format(
                    "Cannot use dynamic tenant in topics for field injected Readers in %s",
                    target
                ));
            }
        } else {
            readerArgument = Argument.of(byte[].class);
            declaredName = injectionPoint.getDeclaringBean().getName();
            target = declaredName;
            if (TopicResolver.isDynamicTenantInTopic(topicValue)) {
                throw new ConfigurationException(String.format(
                    "Cannot use dynamic tenant in topics for field injected Readers in %s",
                    target
                ));
            }
        }

        return getOrCreateReader(annotation, readerArgument, declaredName, target);
    }

    /**
     * Create Pulsar Reader for given injection point if missing.
     *
     * @param annotationValue         value of {@link @PulsarReader} annotation on specified method
     * @param returnType              annotated method return type
     * @param methodInvocationContext context in which annotated method was called
     * @return new instance of Pulsar reader if missing; otherwise return from cache
     */
    @Prototype
    public Reader<?> createReader(@Parameter final AnnotationValue<PulsarReader> annotationValue,
                                  @Parameter final Argument<?> returnType,
                                  @Parameter final MethodInvocationContext<?, ?> methodInvocationContext)
        throws PulsarClientException {

        final String target = methodInvocationContext.getExecutableMethod().getDescription(false);
        final String declaredName = methodInvocationContext.getExecutableMethod().getName();
        return getOrCreateReader(annotationValue, returnType, declaredName, target);
    }

    private Reader<?> getOrCreateReader(final AnnotationValue<PulsarReader> annotation,
                                        final Argument<?> readerArgument,
                                        final String declaredName,
                                        final String target) throws PulsarClientException {
        final Argument<?> keyClass;
        final Argument<?> messageBodyType;
        if (KeyValue.class.isAssignableFrom(readerArgument.getType())) {
            keyClass = readerArgument.getTypeParameters()[0];
            messageBodyType = readerArgument.getTypeParameters()[1];
        } else {
            messageBodyType = readerArgument;
            keyClass = null;
        }

        final TopicResolver.TopicResolved topicResolved = TopicResolver.extractTopic(annotation);
        final String name = annotation.stringValue("readerName").orElse(declaredName);
        final String readerId = topicResolver.generateIdFromMessagingClientName(name, topicResolved);
        if (readers.containsKey(readerId)) {
            return readers.get(readerId);
        }
        final Schema<?> schema = simpleSchemaResolver.decideSchema(messageBodyType, keyClass, annotation, target);
        final String topic = topicResolver.resolve(annotation.getRequiredValue(String.class));

        final MessageId startMessageId;
        if (annotation.getRequiredValue("startMessageLatest", boolean.class)) {
            startMessageId = MessageId.latest;
        } else {
            startMessageId = MessageId.earliest;
        }
        final Reader<?> reader = pulsarClient.newReader(schema)
            .startMessageId(startMessageId)
            .readerName(readerId)
            .topic(topic)
            .create();
        readers.put(readerId, reader);
        return reader;
    }

    @Override
    public void close() {
        for (final Reader<?> reader : readers.values()) {
            try {
                reader.close();
            } catch (Exception e) {
                LOG.warn("Error shutting down Pulsar reader: {}", e.getMessage(), e);
            }
        }
        readers.clear();
    }

    @Override
    public Reader<?> getReader(final String identifier) {
        return readers.get(identifier);
    }

    @Override
    public Collection<Reader<?>> getReaders() {
        return readers.values();
    }
}
