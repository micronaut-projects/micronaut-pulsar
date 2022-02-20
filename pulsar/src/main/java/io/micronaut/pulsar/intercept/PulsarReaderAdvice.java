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
import jakarta.inject.Singleton;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;

import java.util.concurrent.CompletableFuture;

@Singleton
@InterceptorBean(PulsarReader.class)
public class PulsarReaderAdvice implements MethodInterceptor<Object, Object> {

    private final BeanContext beanContext;

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
        final AnnotationValue<PulsarReader> annotationValue = context.getAnnotation(PulsarReader.class);
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
        reader = beanContext.createBean(Reader.class, annotationValue, argumentReturnType);
        if (isAsync) {
            final CompletableFuture<? extends Message<?>> future = reader.readNextAsync();
            if (CompletableFuture.class == returnType.getType()) {
                return future;
            }
            return Publishers.convertPublisher(future, returnType.getType());
        }
        try {
            return reader.readNext();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
}
