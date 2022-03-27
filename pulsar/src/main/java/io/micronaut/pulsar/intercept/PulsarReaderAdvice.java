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
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.type.Argument;
import io.micronaut.core.type.ReturnType;
import io.micronaut.pulsar.annotation.PulsarReader;
import io.micronaut.pulsar.annotation.PulsarReaderClient;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Interceptor for abstract methods annotated with the {@link PulsarReader}.
 *
 * @author Haris Secic
 * @since 1.2.0
 */
@InterceptorBean(PulsarReaderClient.class)
public class PulsarReaderAdvice implements MethodInterceptor<Object, Object> {

    protected final BeanContext beanContext;

    public PulsarReaderAdvice(final BeanContext beanContext) {
        this.beanContext = beanContext;
    }

    @Override
    public Object intercept(final MethodInvocationContext<Object, Object> context) {
        if (!context.hasAnnotation(PulsarReader.class)) {
            return context.proceed();
        }
        if (!context.getExecutableMethod().isAbstract()) {
            throw new IllegalArgumentException(String.format("Non abstract method cannot be annotated as Readers: %s",
                context.getExecutableMethod().getDescription(false)
            ));
        }
        final ReturnType<?> returnType = context.getExecutableMethod().getReturnType();
        final boolean isAsync = returnType.isAsyncOrReactive();
        final Reader<?> reader;
        final Argument<?> argumentReturnType;
        if (isAsync) {
            argumentReturnType = returnType.getFirstTypeVariable().orElseThrow(() -> new IllegalArgumentException(
                "Could not extract return type for %s. Async / reactive "));
        } else {
            argumentReturnType = Argument.of(returnType);
        }
        final AnnotationValue<PulsarReader> annotationValue = context.getAnnotation(PulsarReader.class);
        reader = beanContext.createBean(Reader.class, annotationValue, argumentReturnType);
        if (isAsync) {
            final CompletableFuture<? extends Message<?>> future = reader.readNextAsync();
            if (CompletableFuture.class == returnType.getType()) {
                return future;
            }
            return Publishers.convertPublisher(future, returnType.getType());
        }
        try {
            final int timeout = Objects.requireNonNull(annotationValue).intValue("readTimeout")
                .orElse(0);
            final TimeUnit timeUnit = annotationValue.get("timeoutUnit", TimeUnit.class)
                .orElse(TimeUnit.SECONDS);
            if (timeout != 0) {
                return reader.readNext(timeout, timeUnit);
            }
            return reader.readNext();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
}
