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

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Create pulsar reader beans for fields annotated with PulsarReader.
 *
 * @author Haris Secic
 * @since 1.0
 */
@Factory
public class PulsarReaderFactory implements AutoCloseable, PulsarReaderRegistry {

    private final Map<String, Reader<?>> readers;
    private final PulsarClient pulsarClient;
    private final SchemaResolver schemaResolver;

    public PulsarReaderFactory(PulsarClient pulsarClient, SchemaResolver schemaResolver) {
        this.pulsarClient = pulsarClient;
        this.schemaResolver = schemaResolver;
        this.readers = new ConcurrentHashMap<>();
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

        Optional<Argument<?>> messageBodyType = Optional.empty();
        String declaredName = null;

        if (injectionPoint instanceof ArgumentInjectionPoint) {
            @SuppressWarnings({"unchecked"})
            ArgumentInjectionPoint<?, Reader<?>> argumentInjection = (ArgumentInjectionPoint<?, Reader<?>>) injectionPoint;
            messageBodyType = argumentInjection.getArgument().getFirstTypeVariable();
            declaredName = argumentInjection.getArgument().getName();
        } else if (injectionPoint instanceof FieldInjectionPoint) {
            @SuppressWarnings({"unchecked"})
            FieldInjectionPoint<?, Reader<?>> fieldInjection = (FieldInjectionPoint<?, Reader<?>>) injectionPoint;
            messageBodyType = fieldInjection.asArgument().getFirstTypeVariable();
            declaredName = fieldInjection.getName();
        }

        Class<?> bodyType = messageBodyType.orElse(Argument.of(byte[].class)).getType();

        Schema<?> schema = schemaResolver.decideSchema(annotation, bodyType);
        String name = annotation.stringValue("readerName").orElse(declaredName);
        String topic = annotation.getRequiredValue(String.class);

        boolean startFromLatestMessage = annotation.getRequiredValue("startMessageLatest", boolean.class);
        MessageId startMessageId = startFromLatestMessage ? MessageId.latest : MessageId.earliest;

        return pulsarClient.newReader(schema).startMessageId(startMessageId).readerName(name).topic(topic).create();
    }

    @Override
    public void close() throws Exception {
        for (Reader<?> reader : readers.values()) {
            reader.close();
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
