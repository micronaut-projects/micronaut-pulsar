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

import edu.umd.cs.findbugs.annotations.NonNull;
import io.micronaut.aop.InterceptorBean;
import io.micronaut.aop.MethodInterceptor;
import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.context.BeanContext;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.type.ReturnType;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.pulsar.PulsarProducerRegistry;
import io.micronaut.pulsar.annotation.PulsarProducer;
import io.micronaut.pulsar.annotation.PulsarProducerClient;
import io.micronaut.pulsar.events.ProducerSubscriptionFailedEvent;
import io.micronaut.pulsar.processor.SchemaResolver;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import javax.inject.Singleton;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

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
    private final SchemaResolver schemaResolver;
    private final BeanContext beanContext;
    private final ApplicationEventPublisher applicationEventPublisher;

    public PulsarProducerAdvice(final PulsarClient pulsarClient,
                                final SchemaResolver schemaResolver,
                                final BeanContext beanContext,
                                final ApplicationEventPublisher applicationEventPublisher) {
        this.pulsarClient = pulsarClient;
        this.schemaResolver = schemaResolver;
        this.beanContext = beanContext;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @SuppressWarnings({"rawtypes"})
    public Object intercept(MethodInvocationContext<Object, Object> context) {
        if (!context.hasAnnotation(PulsarProducer.class)) {
            return context.proceed();
        }

        AnnotationValue<PulsarProducer> annotationValue = context.findAnnotation(PulsarProducer.class)
                .orElseThrow(() -> new IllegalStateException("No @PulsarProducer on method: " + context));

        Object value = context.getParameterValues()[0];
        ExecutableMethod<?, ?> method = context.getExecutableMethod();
        Producer producer = getOrCreateProducer(method, annotationValue);
        String producerId = producer.getProducerName();
        ReturnType<?> returnType = method.getReturnType();
        boolean sendBefore = annotationValue.booleanValue("sendBefore").orElse(false);
        boolean isAbstract = context.isAbstract();

        Object returnValue = null;
        if (!isAbstract && !sendBefore) {
            returnValue = context.proceed();
        }

        if (returnType.isAsyncOrReactive()) {
            Object abstractValue = sendAsync(value, producer, returnType);
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
                sendBlocking(value, producer, ReturnType.of(void.class));
                return returnValue;
            }
            return sendBlocking(value, producer, returnType);
        } catch (PulsarClientException e) {
            LOG.error("Failed to produce message on producer {}", producerId, e);
            throw new RuntimeException("Failed to produce a message on " + producerId, e);
        }
    }

    @SuppressWarnings({"rawtypes"})
    private Object sendAsync(Object value, Producer producer, ReturnType<?> returnType) {
        CompletableFuture<?> future = produce(producer, value);
        if (returnType.isAsync()) {
            return future;
        }

        return Publishers.fromCompletableFuture(future.toCompletableFuture());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object sendBlocking(Object value, Producer producer, ReturnType<?> returnType) throws PulsarClientException {
        MessageId sent = producer.send(value);
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

    private Producer<?> getOrCreateProducer(ExecutableMethod<?, ?> method,
                                            AnnotationValue<PulsarProducer> annotationValue) {
        String producerId = annotationValue.stringValue("producerName").orElse(method.getMethodName());
        Producer<?> producer = producers.get(producerId);
        if (null == producer) {
            try {
                producer = beanContext.createBean(Producer.class,
                        pulsarClient,
                        annotationValue,
                        schemaResolver,
                        method.getMethodName(),
                        method.getArguments()[0].getType()
                );
                producers.put(producerId, producer);
            } catch (Exception ex) {
                LOG.error("Failed to create producer {}", producerId);
                applicationEventPublisher.publishEventAsync(new ProducerSubscriptionFailedEvent(producerId, ex));
            }
        }
        return producer;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private CompletableFuture<MessageId> produce(Producer p, Object value) {
        return p.sendAsync(value);
    }

    @Override
    @PreDestroy
    public void close() throws Exception {
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
