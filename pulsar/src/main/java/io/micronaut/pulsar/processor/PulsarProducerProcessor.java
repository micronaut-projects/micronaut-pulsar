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
package io.micronaut.pulsar.processor;

import io.micronaut.context.BeanContext;
import io.micronaut.context.processor.ExecutableMethodProcessor;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.pulsar.annotation.PulsarProducer;
import io.micronaut.pulsar.scope.PulsarClientScope;
import org.apache.pulsar.client.api.PulsarClient;

import javax.inject.Singleton;
import java.util.Objects;

/**
 * Helper class to allow non {@link PulsarClientScope} beans to have their own functions and producers. This class
 * makes it easier to skip boilerplate code and directly access single function for producing messages.
 *
 * @author Haris Secic
 * @since 1.0
 */
@Singleton
public class PulsarProducerProcessor implements ExecutableMethodProcessor<PulsarProducer>, AutoCloseable {

    private final PulsarClientScope scope;

    public PulsarProducerProcessor(PulsarClientScope scope, PulsarClient client) {
        this.scope = scope;
    }

    @Override
    public void process(BeanDefinition<?> beanDefinition, ExecutableMethod<?, ?> method) {
        //scope.createProducer(Objects.requireNonNull(method.getDeclaredAnnotation(PulsarProducer.class)), method);
    }

    @Override
    public void close() throws Exception {

    }
}
