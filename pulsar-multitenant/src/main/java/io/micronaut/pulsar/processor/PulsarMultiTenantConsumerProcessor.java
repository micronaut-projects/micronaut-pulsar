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
package io.micronaut.pulsar.processor;

import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Internal;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.multitenancy.exceptions.TenantNotFoundException;
import io.micronaut.pulsar.annotation.PulsarConsumer;
import io.micronaut.pulsar.config.DefaultPulsarClientConfiguration;
import io.micronaut.pulsar.events.PulsarTenantDiscoveredEvent;
import io.micronaut.runtime.event.annotation.EventListener;
import jakarta.inject.Singleton;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Processor that initializes Pulsar consumers in async fashion if tenant is not specified statically (like when
 * hardcoded as a value in {@link PulsarConsumer} or specified as default in
 * {@link io.micronaut.pulsar.config.PulsarClientConfiguration}.
 *
 * @author Haris
 * @since 1.2.0
 */
@Internal
@Replaces(bean = PulsarConsumerProcessor.class)
@Singleton
final class PulsarMultiTenantConsumerProcessor extends PulsarConsumerProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarConsumerProcessor.class);

    private final Map<String, MultiTenantConsumer> multiTenantConsumers;
    private final TenantNameResolver tenantNameResolver;

    public PulsarMultiTenantConsumerProcessor(final ApplicationEventPublisher<Object> applicationEventPublisher,
                                              final BeanContext beanContext,
                                              final PulsarClient pulsarClient,
                                              final DefaultSchemaHandler simpleSchemaResolver,
                                              final DefaultPulsarClientConfiguration pulsarClientConfiguration,
                                              final TopicResolver topicResolver,
                                              final TenantNameResolver tenantNameResolver) {
        super(applicationEventPublisher,
            beanContext,
            pulsarClient,
            simpleSchemaResolver,
            pulsarClientConfiguration,
            topicResolver);
        this.tenantNameResolver = tenantNameResolver;
        if (!tenantNameResolver.isStaticTenantResolver()) {
            multiTenantConsumers = new ConcurrentHashMap<>(10);
        } else {
            multiTenantConsumers = null;
        }
    }

    /**
     * Instantiate Pulsar consumer if tenant is available; otherwise put queue and exit. If tenant becomes available
     * and  {@link PulsarTenantDiscoveredEvent} is published queue is re-read and for each defined consumer new Apache
     * Pulsar consumer is created and started.
     *
     * @param beanDefinition definition of a bean that declares method annotated with {@link PulsarConsumer}
     * @param method         executable method that serves as a message consumer
     */
    @Override
    public void process(final BeanDefinition<?> beanDefinition, final ExecutableMethod<?, ?> method) {
        try {
            final AnnotationValue<PulsarConsumer> annotation = method.getAnnotation(PulsarConsumer.class);
            final TopicResolver.TopicResolved topic = TopicResolver.extractTopic(Objects.requireNonNull(annotation));
            if (!topic.isDynamicTenant() || tenantNameResolver.hasTenantName()) {
                super.process(beanDefinition, method);
                return;
            }
            final String consumerId = getConsumerName(annotation);
            if (!multiTenantConsumers.containsKey(consumerId)) {
                multiTenantConsumers.put(consumerId, new MultiTenantConsumer(beanDefinition, method));
            }
        } catch (final TenantNotFoundException | NullPointerException ex) {
            if (ex instanceof TenantNotFoundException) {
                LOG.warn("Failed to instantiate a bean with consumers because topic value was set to dynamic tenant while tenant was missing.", ex);
            }
        }
    }

    @EventListener
    public void resolveNewTenant(final PulsarTenantDiscoveredEvent event) {
        tenantNameResolver.overrideTenantName(tenantNameResolver.resolveTenantNameFromId(event.getTenant()));
        for (final MultiTenantConsumer x : multiTenantConsumers.values()) {
            super.process(x.getBeanDefinition(), x.getMethod());
        }
        tenantNameResolver.clearTenantName();
    }

    private static final class MultiTenantConsumer {
        private final BeanDefinition<?> beanDefinition;
        private final ExecutableMethod<?, ?> method;

        public MultiTenantConsumer(BeanDefinition<?> beanDefinition, ExecutableMethod<?, ?> method) {
            this.beanDefinition = beanDefinition;
            this.method = method;
        }

        public BeanDefinition<?> getBeanDefinition() {
            return beanDefinition;
        }

        public ExecutableMethod<?, ?> getMethod() {
            return method;
        }
    }
}
