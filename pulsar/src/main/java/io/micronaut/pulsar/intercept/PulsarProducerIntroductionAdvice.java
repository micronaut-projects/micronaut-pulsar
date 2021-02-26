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
import io.micronaut.aop.MethodInterceptor;
import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.context.BeanContext;
import io.micronaut.context.processor.ExecutableMethodProcessor;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.type.ReturnType;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.pulsar.PulsarProducerRegistry;
import io.micronaut.pulsar.annotation.PulsarProducer;
import io.micronaut.pulsar.processor.SchemaResolver;
import io.reactivex.Flowable;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import javax.inject.Singleton;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Intercepting pulsar {@link Producer} methods.
 *
 * @author Haris Secic
 * @since 1.0
 */
@Singleton
public final class PulsarProducerIntroductionAdvice implements MethodInterceptor<Object, Object>,
        AutoCloseable,
        PulsarProducerRegistry, ExecutableMethodProcessor<PulsarProducer> {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarProducerIntroductionAdvice.class);

    private final Map<String, Producer<?>> producers = new ConcurrentHashMap<>();
    private final PulsarClient pulsarClient;
    private final SchemaResolver schemaResolver;
    private final BeanContext beanContext;

    public PulsarProducerIntroductionAdvice(final PulsarClient pulsarClient,
                                            final SchemaResolver schemaResolver,
                                            final BeanContext beanContext) {
        this.pulsarClient = pulsarClient;
        this.schemaResolver = schemaResolver;
        this.beanContext = beanContext;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
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

        if (returnType.isAsyncOrReactive()) {
            if (returnType.isAsync()) {
                return produce(producer, value);
            }
            if (returnType.isReactive()) {
                Flowable<?> resulting = Flowable.fromFuture(produce(producer, value));
                return Publishers.convertPublisher(resulting, returnType.getType());
            }
        }

        try {
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

            throw new IllegalArgumentException("Pulsar producers can only return MessageId or body being sent.");
        } catch (PulsarClientException e) {
            LOG.error("Failed to produce message on producer {}", producerId, e);
            throw new RuntimeException("Failed to produce a message on " + producerId, e);
        }
    }

    private Producer<?> getOrCreateProducer(ExecutableMethod<?, ?> method,
                                            AnnotationValue<PulsarProducer> annotationValue) {
        String producerId = annotationValue.stringValue("producerName").orElse(method.getMethodName());
        Producer<?> producer = producers.get(producerId);
        if (null == producer) {
            producer = beanContext.createBean(Producer.class,
                    pulsarClient,
                    annotationValue,
                    schemaResolver,
                    method.getMethodName(),
                    method.getArguments()[0]
            );
            producers.put(producerId, producer);
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

    @Override
    public void process(BeanDefinition<?> beanDefinition, ExecutableMethod<?, ?> method) {
        getOrCreateProducer(method, Objects.requireNonNull(method.getDeclaredAnnotation(PulsarProducer.class)));
    }
}
