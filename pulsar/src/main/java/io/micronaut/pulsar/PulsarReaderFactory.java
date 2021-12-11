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

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Prototype;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.type.Argument;
import io.micronaut.inject.ArgumentInjectionPoint;
import io.micronaut.inject.FieldInjectionPoint;
import io.micronaut.inject.InjectionPoint;
import io.micronaut.pulsar.annotation.PulsarReader;
import io.micronaut.pulsar.processor.SchemaResolver;
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
    private final SchemaResolver schemaResolver;

    public PulsarReaderFactory(PulsarClient pulsarClient, SchemaResolver schemaResolver) {
        this.pulsarClient = pulsarClient;
        this.schemaResolver = schemaResolver;
    }

    /**
     * Create Pulsar Reader for given injection point if missing.
     *
     * @param injectionPoint field or argument injection point
     * @return new instance of Pulsar reader for each injection point
     * @throws PulsarClientException in case of not being able to create such Reader
     */
    @Prototype
    public Reader<?> createReader(InjectionPoint<?> injectionPoint) throws PulsarClientException {

        AnnotationValue<PulsarReader> annotation = injectionPoint.getAnnotation(PulsarReader.class);
        if (null == annotation) { //is this state possible?
            throw new IllegalStateException("Failed to get value for bean annotated with PulsarReader");
        }

        final Argument<?> messageBodyType;
        final Argument<?> keyClass;
        final Argument<?> readerArgument;
        String declaredName = null;

        if (injectionPoint instanceof ArgumentInjectionPoint) {
            @SuppressWarnings({"unchecked"})
            ArgumentInjectionPoint<?, Reader<?>> argumentInjection = (ArgumentInjectionPoint<?, Reader<?>>) injectionPoint;
            readerArgument = argumentInjection.getArgument().getFirstTypeVariable()
                    .orElse(Argument.of(byte[].class));
            declaredName = argumentInjection.getArgument().getName();
        } else if (injectionPoint instanceof FieldInjectionPoint) {
            @SuppressWarnings({"unchecked"})
            FieldInjectionPoint<?, Reader<?>> fieldInjection = (FieldInjectionPoint<?, Reader<?>>) injectionPoint;
            readerArgument = fieldInjection.asArgument().getFirstTypeVariable()
                    .orElse(Argument.of(byte[].class));
            declaredName = fieldInjection.getName();
        } else {
            readerArgument = Argument.of(byte[].class);
        }

        if (KeyValue.class.isAssignableFrom(readerArgument.getType())) {
            keyClass = readerArgument.getTypeParameters()[0];
            messageBodyType = readerArgument.getTypeParameters()[1];
        } else {
            messageBodyType = readerArgument;
            keyClass = null;
        }

        final Schema<?> schema = schemaResolver.decideSchema(messageBodyType, keyClass, annotation);
        final String name = annotation.stringValue("readerName").orElse(declaredName);
        final String topic = annotation.getRequiredValue(String.class);

        boolean startFromLatestMessage = annotation.getRequiredValue("startMessageLatest", boolean.class);
        MessageId startMessageId = startFromLatestMessage ? MessageId.latest : MessageId.earliest;

        return pulsarClient
                .newReader(schema)
                .startMessageId(startMessageId)
                .readerName(name)
                .topic(topic)
                .create();
    }

    @Override
    public void close() throws Exception {
        for (Reader<?> reader : readers.values()) {
            try {
                reader.close();
            } catch (Exception e) {
                LOG.warn("Error shutting down Pulsar reader: {}", e.getMessage(), e);
            }
        }
        readers.clear();
    }

    @Override
    public Reader<?> getReader(String identifier) {
        return readers.get(identifier);
    }

    @Override
    public Collection<Reader<?>> getReaders() {
        return readers.values();
    }
}
