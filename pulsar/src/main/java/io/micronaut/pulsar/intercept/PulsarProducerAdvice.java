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
import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.context.BeanContext;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.type.ArgumentValue;
import io.micronaut.core.type.MutableArgumentValue;
import io.micronaut.core.type.ReturnType;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.messaging.annotation.MessageBody;
import io.micronaut.messaging.annotation.MessageHeader;
import io.micronaut.messaging.exceptions.MessageListenerException;
import io.micronaut.pulsar.PulsarProducerRegistry;
import io.micronaut.pulsar.annotation.*;
import io.micronaut.pulsar.events.ProducerSubscriptionFailedEvent;
import io.micronaut.pulsar.processor.DefaultSchemaHandler;
import jakarta.annotation.PreDestroy;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.schema.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Intercepting pulsar {@link Producer} methods. It can be used for creating implementation of interface methods or
 * just to add producer behaviour to existing methods.
 *
 * @author Haris Secic
 * @since 1.0
 */
@InterceptorBean(PulsarProducerClient.class)
public class PulsarProducerAdvice implements MethodInterceptor<Object, Object>, AutoCloseable, PulsarProducerRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarProducerAdvice.class);

    protected final Map<String, Producer<?>> producers = new ConcurrentHashMap<>();
    protected final PulsarClient pulsarClient;
    protected final DefaultSchemaHandler simpleSchemaResolver;
    protected final BeanContext beanContext;
    protected final ApplicationEventPublisher<ProducerSubscriptionFailedEvent> applicationEventPublisher;
    protected final ConversionService conversionService;

    /**
     * Constructor for instantiating Pulsar producer advice for intercepting producer methods.
     *
     * @param pulsarClient              Apache Pulsar client bean
     * @param simpleSchemaResolver      Schema resolver
     * @param beanContext               Micronaut bean context
     * @param applicationEventPublisher Event publisher for reporting failed subscriptions
     * @param conversionService         Micronaut conversion service
     */
    public PulsarProducerAdvice(final PulsarClient pulsarClient,
                                final DefaultSchemaHandler simpleSchemaResolver,
                                final BeanContext beanContext,
                                final ApplicationEventPublisher<ProducerSubscriptionFailedEvent> applicationEventPublisher,
                                final ConversionService conversionService) {
        this.pulsarClient = pulsarClient;
        this.simpleSchemaResolver = simpleSchemaResolver;
        this.beanContext = beanContext;
        this.applicationEventPublisher = applicationEventPublisher;
        this.conversionService = conversionService;
    }

    @Override
    public Object intercept(final MethodInvocationContext<Object, Object> context) {
        if (!context.hasAnnotation(PulsarProducer.class)) {
            return context.proceed();
        }

        AnnotationValue<PulsarProducer> annotationValue = context.findAnnotation(PulsarProducer.class)
            .orElseThrow(() -> new IllegalStateException("No @PulsarProducer on method: " + context));

        boolean sendBefore = annotationValue.booleanValue("sendBefore").orElse(false);
        boolean isAbstract = context.isAbstract();

        // store value of the call before
        final Object returnValue = !isAbstract && !sendBefore ? context.proceed() : null;

        final Object value = getValueFromContext(context);
        final Object key = getKeyFromContext(context);
        final Map<String, String> headers = collectHeaders(context);
        final ExecutableMethod<?, ?> method = context.getExecutableMethod();
        final Producer<?> producer = getOrCreateProducer(method, annotationValue);
        final ReturnType<?> returnType = method.getReturnType();

        if (returnType.isAsyncOrReactive()) {
            Object abstractValue = sendAsync(value, producer, returnType, key, headers);
            if (isAbstract) {
                return abstractValue;
            }
            if (!sendBefore) {
                return returnValue;
            }
            return context.proceed();
        }

        try {
            if (!isAbstract) {
                sendBlocking(value, producer, ReturnType.of(void.class), key, headers);
                return returnValue;
            }
            return sendBlocking(value, producer, returnType, key, headers);
        } catch (PulsarClientException e) {
            String producerId = producer.getProducerName();
            LOG.error("Failed to produce message on producer {}", producerId, e);
            throw new RuntimeException("Failed to produce a message on " + producerId, e);
        }
    }

    @NonNull
    private static Object getValueFromContext(MethodInvocationContext<Object, Object> context) {
        if (context.getParameters().size() == 1) {
            return context.getParameterValues()[0];
        }
        return context.getParameters().values().stream()
            .filter(mutableArgumentValue -> mutableArgumentValue.isAnnotationPresent(MessageBody.class))
            .map(ArgumentValue::getValue)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(
                "Producers with multiple values must have one argument annotated with @MessageBody"));
    }

    @Nullable
    private static Object getKeyFromContext(MethodInvocationContext<Object, Object> context) {
        if (context.getParameters().size() == 1) {
            return null;
        }
        return context.getParameters().values().stream()
            .filter(mutableArgumentValue -> mutableArgumentValue.isAnnotationPresent(MessageKey.class))
            .map(ArgumentValue::getValue)
            .findFirst()
            .orElse(null);
    }

    private <T, V> Object sendAsync(final V value,
                                           final Producer<T> producer,
                                           final ReturnType<?> returnType,
                                           final @Nullable Object key,
                                           final Map<String, String> headers) {
        final CompletableFuture<?> future = buildMessage(producer, value, key, headers).sendAsync();
        if (CompletableFuture.class == returnType.getType()) {
            return future;
        }
        return Publishers.convertPublisher(conversionService, future, returnType.getType());
    }

    private static <T, V> Object sendBlocking(final V value,
                                              final Producer<T> producer,
                                              final ReturnType<?> returnType,
                                              final @Nullable Object key,
                                              final Map<String, String> headers) throws PulsarClientException {
        final MessageId sent = buildMessage(producer, value, key, headers).send();
        if (returnType.isVoid()) {
            return Void.TYPE;
        }

        if (returnType.getType() == MessageId.class) {
            return sent;
        }

        if (returnType.getType() == value.getClass()) {
            return value;
        }

        throw new IllegalArgumentException("Pulsar abstract producers can only return MessageId or body being sent.");
    }

    @SuppressWarnings("unchecked")
    private static <T, V> TypedMessageBuilder<?> buildMessage(final Producer<T> producer,
                                                              final V value,
                                                              final @Nullable Object key,
                                                              final Map<String, String> headers) {
        final TypedMessageBuilder<T> message = producer.newMessage();
        if (null == key) {
            message.value((T) value);
        } else {
            // due to Pulsar library value will anyway require type of KV instead of just simple value
            // also if key encoding is SEPARATE, setting key individually (builder.key()) will not work
            message.value((T) new KeyValue<>(key, value));
        }
        if (!headers.isEmpty()) {
            message.properties(headers);
        }
        return message;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> collectHeaders(final MethodInvocationContext<Object, Object> context) {
        if (context.getParameters().size() == 1) {
            return Collections.emptyMap();
        }
        final List<MutableArgumentValue<?>> headers = context.getParameters().values().stream()
            .filter(x -> x.isAnnotationPresent(MessageProperties.class) || x.isAnnotationPresent(MessageHeader.class))
            .toList();
        if (headers.size() == 1 && headers.get(0).isAnnotationPresent(MessageProperties.class)) {
            return (Map<String, String>) headers.get(0).getValue();
        }
        return headers.stream().collect(Collectors.toMap(
            x -> Objects.requireNonNull(x.getAnnotation(MessageHeader.class)).stringValue().orElse(x.getName()),
            x -> (String) x.getValue()
        ));
    }

    /**
     * Fetch existing producer or generate a new one via factory if missing.
     * @param method method annotated with {@link PulsarProducer}
     * @param annotationValue {@link PulsarProducer} value
     * @return existing producer if exists; otherwise create a new one
     */
    protected Producer<?> getOrCreateProducer(final ExecutableMethod<?, ?> method,
                                              final AnnotationValue<PulsarProducer> annotationValue) {
        final String producerId = annotationValue.stringValue("producerName").orElse(method.getMethodName());
        Producer<?> producer = producers.get(producerId);
        if (null == producer) {
            try {
                producer = beanContext.createBean(Producer.class,
                    pulsarClient,
                    annotationValue,
                    method.getArguments(),
                    simpleSchemaResolver,
                    method.getDescription(true)
                );
                producers.put(producerId, producer);
            } catch (Exception ex) {
                if (MessageListenerException.class == ex.getClass() && ex.getMessage().startsWith("Topic")) {
                    LOG.error("Topic missing for producer {} {}", producerId, method.getDescription(false));
                } else {
                    LOG.error("Failed to create producer {} with reason: ", producerId, ex);
                }
                applicationEventPublisher.publishEventAsync(new ProducerSubscriptionFailedEvent(producerId, ex));
            }
        }
        return producer;
    }

    @Override
    @PreDestroy
    public void close() {
        for (Producer<?> producer : producers.values()) {
            if (producer.isConnected()) {
                try {
                    producer.flush();
                    producer.close();
                } catch (Exception e) {
                    LOG.warn("Error shutting down Pulsar producer: {}", e.getMessage(), e);
                }
            }
        }
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
