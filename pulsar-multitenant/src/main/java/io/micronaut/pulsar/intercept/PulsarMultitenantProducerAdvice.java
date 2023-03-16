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
package io.micronaut.pulsar.intercept;

import io.micronaut.aop.InterceptorBean;
import io.micronaut.aop.MethodInterceptor;
import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.messaging.exceptions.MessageListenerException;
import io.micronaut.pulsar.annotation.PulsarProducer;
import io.micronaut.pulsar.annotation.PulsarProducerClient;
import io.micronaut.pulsar.config.PulsarClientConfiguration;
import io.micronaut.pulsar.events.ProducerSubscriptionFailedEvent;
import io.micronaut.pulsar.processor.DefaultSchemaHandler;
import io.micronaut.pulsar.processor.TopicResolver;
import jakarta.annotation.PreDestroy;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Intercepting pulsar {@link Producer} methods. It can be used for creating implementation of interface methods or
 * just to add producer behaviour to existing methods.
 *
 * @author Haris Secic
 * @since 1.0
 */
@InterceptorBean(PulsarProducerClient.class)
@Replaces(PulsarProducerAdvice.class)
public final class PulsarMultitenantProducerAdvice extends PulsarProducerAdvice
    implements MethodInterceptor<Object, Object>, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarMultitenantProducerAdvice.class);

    private final TopicResolver topicResolver;
    private final boolean shutdownOnSubscribeError;

    public PulsarMultitenantProducerAdvice(final PulsarClient pulsarClient,
                                           final DefaultSchemaHandler simpleSchemaResolver,
                                           final BeanContext beanContext,
                                           final ApplicationEventPublisher<ProducerSubscriptionFailedEvent> applicationEventPublisher,
                                           final TopicResolver topicResolver,
                                           final ConversionService conversionService,
                                           final PulsarClientConfiguration configuration) {
        super(pulsarClient, simpleSchemaResolver, beanContext, applicationEventPublisher, conversionService);
        this.topicResolver = topicResolver;
        this.shutdownOnSubscribeError = configuration.getShutdownOnSubscriberError();
    }

    @Override
    protected Producer<?> getOrCreateProducer(final ExecutableMethod<?, ?> method,
                                              final AnnotationValue<PulsarProducer> annotationValue) {
        final var producerName = annotationValue.stringValue("producerName", null)
            .orElse(method.getDescription(true));
        final var topicResolved = TopicResolver.extractTopic(annotationValue,
            producerName,
            shutdownOnSubscribeError);
        final var producerId = topicResolver.generateIdFromMessagingClientName(producerName, topicResolved);
        return producers.computeIfAbsent(producerId,
            id -> tryCreate(beanContext, annotationValue, method, id));
    }

    private Producer<?> tryCreate(final BeanContext beanContext,
                                  final AnnotationValue<PulsarProducer> annotationValue,
                                  final ExecutableMethod<?, ?> method,
                                  final String producerId) {
        try {
            return beanContext.createBean(Producer.class,
                pulsarClient,
                annotationValue,
                method.getArguments(),
                simpleSchemaResolver,
                method.getDescription(true));
        } catch (Exception ex) {
            if (MessageListenerException.class == ex.getClass() && ex.getMessage().startsWith("Topic")) {
                LOG.error("Topic missing for producer {} {}", producerId, method.getDescription(false));
            } else {
                LOG.error("Failed to create producer {} with reason: ", producerId, ex);
            }
            applicationEventPublisher.publishEventAsync(new ProducerSubscriptionFailedEvent(producerId, ex));
            return null;
        }
    }

    @Override
    @PreDestroy
    public void close() {
        producers.values().stream().filter(Producer::isConnected).forEach(producer -> {
            try {
                producer.flush();
                producer.close();
            } catch (Exception e) {
                LOG.warn("Error shutting down Pulsar producer: {}", e.getMessage(), e);
            }
        });
    }

    @Override
    public Map<String, Producer<?>> getProducers() {
        return producers;
    }

    @Override
    public Producer<?> getProducer(@NonNull String id) {
        return producers.get(id);
    }

    @Override
    public Set<String> getProducerIds() {
        return producers.keySet();
    }
}
