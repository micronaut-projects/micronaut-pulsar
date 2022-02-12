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
package io.micronaut.pulsar.intercept;

import io.micronaut.aop.InterceptorBean;
import io.micronaut.aop.MethodInterceptor;
import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.context.BeanContext;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.context.exceptions.BeanInstantiationException;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.type.ArgumentValue;
import io.micronaut.core.type.MutableArgumentValue;
import io.micronaut.core.type.ReturnType;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.messaging.annotation.MessageBody;
import io.micronaut.messaging.annotation.MessageHeader;
import io.micronaut.messaging.exceptions.MessageListenerException;
import io.micronaut.pulsar.PulsarProducerRegistry;
import io.micronaut.pulsar.annotation.MessageKey;
import io.micronaut.pulsar.annotation.MessageProperties;
import io.micronaut.pulsar.annotation.PulsarProducer;
import io.micronaut.pulsar.annotation.PulsarProducerClient;
import io.micronaut.pulsar.events.ProducerSubscriptionFailedEvent;
import io.micronaut.pulsar.processor.DefaultSchemaHandler;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
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
@Singleton
@InterceptorBean(PulsarProducerClient.class)
public final class PulsarProducerAdvice implements MethodInterceptor<Object, Object>,
        AutoCloseable, PulsarProducerRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarProducerAdvice.class);

    private final Map<String, Producer<?>> producers = new ConcurrentHashMap<>();
    private final PulsarClient pulsarClient;
    private final DefaultSchemaHandler simpleSchemaResolver;
    private final BeanContext beanContext;
    private final ApplicationEventPublisher<ProducerSubscriptionFailedEvent> applicationEventPublisher;

    public PulsarProducerAdvice(final PulsarClient pulsarClient,
                                final DefaultSchemaHandler simpleSchemaResolver,
                                final BeanContext beanContext,
                                final ApplicationEventPublisher<ProducerSubscriptionFailedEvent> applicationEventPublisher) {
        this.pulsarClient = pulsarClient;
        this.simpleSchemaResolver = simpleSchemaResolver;
        this.beanContext = beanContext;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    public Object intercept(MethodInvocationContext<Object, Object> context) {
        if (!context.hasAnnotation(PulsarProducer.class)) {
            return context.proceed();
        }

        AnnotationValue<PulsarProducer> annotationValue = context.findAnnotation(PulsarProducer.class)
                .orElseThrow(() -> new IllegalStateException("No @PulsarProducer on method: " + context));

        boolean sendBefore = annotationValue.booleanValue("sendBefore").orElse(false);
        boolean isAbstract = context.isAbstract();

        final Object returnValue; // store value of the call before

        if (!isAbstract && !sendBefore) {
            returnValue = context.proceed();
        } else {
            returnValue = null;
        }

        final Object value, key;
        final Map<String, String> headers;
        if (context.getParameters().size() == 1) {
            value = context.getParameterValues()[0];
            key = null;
            headers = Collections.emptyMap();
        } else {
            value = context.getParameters().values().stream()
                    .filter(mutableArgumentValue -> mutableArgumentValue.isAnnotationPresent(MessageBody.class))
                    .map(ArgumentValue::getValue)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Producers with multiple values must have one argument annotated with @MessageBody"));
            //key can be omitted if headers are used instead of just payload
            key = context.getParameters().values().stream()
                    .filter(mutableArgumentValue -> mutableArgumentValue.isAnnotationPresent(MessageKey.class))
                    .map(ArgumentValue::getValue)
                    .findFirst()
                    .orElse(null);
            headers = collectHeaders(context);
        }
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

    private <T, V> Object sendAsync(V value, Producer<T> producer, ReturnType<?> returnType, @Nullable Object key, Map<String, String> headers) {
        final CompletableFuture<?> future = buildMessage(producer, value, key, headers).sendAsync();
        if (CompletableFuture.class == returnType.getType()) {
            return future;
        }
        return Publishers.convertPublisher(future, returnType.getType());
    }

    private <T, V> Object sendBlocking(V value,
                                       Producer<T> producer,
                                       ReturnType<?> returnType,
                                       @Nullable Object key,
                                       Map<String, String> headers) throws PulsarClientException {
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
    private static <T, V> TypedMessageBuilder<?> buildMessage(Producer<T> producer,
                                                              V value,
                                                              @Nullable Object key,
                                                              Map<String, String> headers) {
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
        final List<MutableArgumentValue<?>> headers = context.getParameters().values().stream()
                .filter(x -> x.isAnnotationPresent(MessageProperties.class) || x.isAnnotationPresent(MessageHeader.class))
                .collect(Collectors.toList());
        if (headers.size() == 1 && headers.get(0).isAnnotationPresent(MessageProperties.class)) {
            return (Map<String, String>) headers.get(0).getValue();
        }
        return headers.stream().collect(Collectors.toMap(
                x -> Objects.requireNonNull(x.getAnnotation(MessageHeader.class)).stringValue().orElse(x.getName()),
                x -> (String) x.getValue()
        ));
    }

    private Producer<?> getOrCreateProducer(ExecutableMethod<?, ?> method,
                                            AnnotationValue<PulsarProducer> annotationValue) {
        String producerId = annotationValue.stringValue("producerName").orElse(method.getMethodName());
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
