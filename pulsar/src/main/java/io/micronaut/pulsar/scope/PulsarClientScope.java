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
package io.micronaut.pulsar.scope;

import io.micronaut.context.BeanContext;
import io.micronaut.context.BeanResolutionContext;
import io.micronaut.context.LifeCycle;
import io.micronaut.context.scope.CustomScope;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.BeanIdentifier;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.pulsar.annotation.PulsarProducer;
import io.micronaut.pulsar.annotation.PulsarProducerClient;
import io.micronaut.pulsar.processor.SchemaResolver;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A scope implementation for injecting {@link PulsarProducerClient} instances.
 *
 * @author Haris Secic
 * @since 1.0
 */
@Singleton
public class PulsarClientScope implements CustomScope<PulsarProducerClient>, LifeCycle<PulsarClientScope> {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarClientScope.class);

    private final Map<String, List<Producer<?>>> producersCollection;
    private final BeanContext beanContext;
    private final SchemaResolver schemaResolver;
    private final PulsarClient pulsarClient;

    public PulsarClientScope(BeanContext beanContext, SchemaResolver schemaResolver,
                             PulsarClient pulsarClient) {
        this.beanContext = beanContext;
        this.schemaResolver = schemaResolver;
        this.pulsarClient = pulsarClient;
        producersCollection = new ConcurrentHashMap<>();
    }

    @Override
    public Class<PulsarProducerClient> annotationType() {
        return PulsarProducerClient.class;
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public <T> T get(BeanResolutionContext resolutionContext, BeanDefinition<T> beanDefinition,
                     BeanIdentifier identifier, Provider<T> provider) {
        List<Producer<?>> producerMethods = new ArrayList<>();
        for (ExecutableMethod<T, ?> x : beanDefinition.getExecutableMethods()) {
            AnnotationValue<PulsarProducer> annotation = x.getAnnotation(PulsarProducer.class);
            if (null != annotation) {
                producerMethods.add(createProducer(annotation));
            }
        }
        return (T) producersCollection.computeIfAbsent(identifier.getName(), key -> producerMethods);
    }

    private Producer<?> createProducer(AnnotationValue<PulsarProducer> annotationValue) {
        String producerId = annotationValue.getRequiredValue("producerName", String.class);
        //Annotation processor for @PulsarProducer will also create such beans. Avoid having duplicates.
        if (beanContext.containsBean(Producer.class, Qualifiers.byName(producerId))) {
            return beanContext.getBean(Producer.class, Qualifiers.byName(producerId));
        }
        return beanContext.createBean(Producer.class, Qualifiers.byName(producerId),
                pulsarClient, annotationValue, schemaResolver);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Optional<T> remove(BeanIdentifier identifier) {
        String id = identifier.getName();
        List<Producer<?>> producers = producersCollection.remove(id);
        if (null != producers) {
            for (Producer<?> producer : producers) {
                producer.closeAsync().handle((v, ex) -> {
                    if (null != ex) {
                        LOG.warn("Error shutting down Pulsar producer: {}", producer.getProducerName());
                    }
                    return v;
                });
            }
            return Optional.of((T) producers);
        }
        return Optional.empty();
    }

    @Override
    public PulsarClientScope stop() {
        for (List<Producer<?>> producers : producersCollection.values()) {
            for (Producer<?> producer : producers) {
                try {
                    producer.close();
                } catch (PulsarClientException e) {
                    LOG.warn("Error shutting down Pulsar producer: {}", e.getMessage(), e);
                }
            }
        }
        producersCollection.clear();
        return this;
    }

    @Override
    public boolean isRunning() {
        return true;
    }
}
