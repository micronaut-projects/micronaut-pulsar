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
package io.micronaut.pulsar.intercept;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.micronaut.aop.MethodInterceptor;
import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.context.BeanContext;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.type.ReturnType;
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Intercepting pulsar {@link Producer} methods.
 */
@Singleton
public final class PulsarClientIntroductionAdvice implements MethodInterceptor<Object, Object>, AutoCloseable, PulsarProducerRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarClientIntroductionAdvice.class);

    private final Map<String, Producer<?>> producers;
    private final PulsarClient pulsarClient;
    private final SchemaResolver schemaResolver;
    private final BeanContext beanContext;

    public PulsarClientIntroductionAdvice(final PulsarClient pulsarClient,
                                          final SchemaResolver schemaResolver,
                                          final BeanContext beanContext) {
        this.pulsarClient = pulsarClient;
        this.schemaResolver = schemaResolver;
        this.beanContext = beanContext;
        producers = new ConcurrentHashMap<>();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object intercept(MethodInvocationContext<Object, Object> context) {
        if (context.hasAnnotation(PulsarProducer.class)) {
            AnnotationValue<PulsarProducer> annotationValue = context.findAnnotation(PulsarProducer.class)
                    .orElseThrow(() -> new IllegalStateException("No @PulsarProducer on method: " + context));

            String producerId = annotationValue.getRequiredValue("producerName", String.class);
            Producer producer = producers.get(producerId);

            if (null == producer) {
                producer = beanContext.createBean(Producer.class, pulsarClient, annotationValue, schemaResolver);
                producers.put(producerId, producer);
            }

            ReturnType<?> returnType = context.getReturnType();
            Object value = context.getParameterValues()[0];

            if (returnType.isAsyncOrReactive()) {
                if (returnType.isAsync()) {
                    return produce(producer, value);
                }
                if (returnType.isReactive()) {
                    Flowable resulting = Flowable.fromFuture(produce(producer, value));
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

                Class<?> bodyType = annotationValue.getRequiredValue("bodyType", Class.class);
                if (returnType.getType() == bodyType) {
                    return value;
                }

                throw new IllegalArgumentException("Pulsar producers can only return MessageId or body being sent.");
            } catch (PulsarClientException e) {
                if (LOG.isErrorEnabled()) {
                    LOG.error("Failed to produce message on producer " + producerId, e);
                }
                throw new RuntimeException("Failed to produce a message on " + producerId, e);
            }
        }
        return context.proceed();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private CompletableFuture<MessageId> produce(Producer p, Object value) {
        return p.sendAsync(value);
    }

    @Override
    @PreDestroy
    public void close() throws Exception {
        for (Producer<?> producer : this.producers.values()) {
            if (producer.isConnected()) {
                producer.flush();
                producer.close();
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
