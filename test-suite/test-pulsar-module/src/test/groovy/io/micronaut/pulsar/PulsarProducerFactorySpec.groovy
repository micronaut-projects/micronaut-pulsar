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
package io.micronaut.pulsar

import io.micronaut.core.annotation.AnnotationValue
import io.micronaut.core.type.Argument
import io.micronaut.pulsar.annotation.PulsarProducer
import io.micronaut.pulsar.processor.DefaultSchemaHandler
import io.micronaut.pulsar.processor.TopicResolver
import org.apache.pulsar.client.impl.ProducerBuilderImpl
import spock.lang.Specification

class PulsarProducerFactorySpec extends Specification {

    private AnnotationValue<PulsarProducer> createAnnotation(boolean batching, boolean blockQueue) {
        return AnnotationValue.builder(PulsarProducer)
                .member("topic", "persistent://public/default/test-topic")
                .member("schema", MessageSchema.BYTES)
                .member("batching", batching)
                .member("blockQueue", blockQueue)
                .build()
    }

    void "test producer batching enabled by default and blockQueue not affected"() {
        given:
        TopicResolver topicResolver = { String topic -> topic } as TopicResolver
        DefaultSchemaHandler schemaHandler = new DefaultSchemaHandler(null)
        PulsarProducerFactory factory = new PulsarProducerFactory(topicResolver)
        Argument<?>[] args = [Argument.of(byte[], "message")] as Argument<?>[]
        AnnotationValue<PulsarProducer> annotationValue = createAnnotation(true, false)

        when:
        ProducerBuilderImpl<?> builder = (ProducerBuilderImpl<?>) factory.createProducerBuilder(
                null,
                annotationValue,
                args,
                schemaHandler,
                "testMethod"
        )

        then:
        builder.conf.batchingEnabled
        !builder.conf.blockIfQueueFull
    }

    void "test producer batching disabled does not alter blockIfQueueFull"() {
        given:
        TopicResolver topicResolver = { String topic -> topic } as TopicResolver
        DefaultSchemaHandler schemaHandler = new DefaultSchemaHandler(null)
        PulsarProducerFactory factory = new PulsarProducerFactory(topicResolver)
        Argument<?>[] args = [Argument.of(byte[], "message")] as Argument<?>[]
        AnnotationValue<PulsarProducer> annotationValue = createAnnotation(false, true)

        when:
        ProducerBuilderImpl<?> builder = (ProducerBuilderImpl<?>) factory.createProducerBuilder(
                null,
                annotationValue,
                args,
                schemaHandler,
                "testMethod"
        )

        then:
        !builder.conf.batchingEnabled
        builder.conf.blockIfQueueFull
    }

    void "test producer batching and blockQueue both disabled"() {
        given:
        TopicResolver topicResolver = { String topic -> topic } as TopicResolver
        DefaultSchemaHandler schemaHandler = new DefaultSchemaHandler(null)
        PulsarProducerFactory factory = new PulsarProducerFactory(topicResolver)
        Argument<?>[] args = [Argument.of(byte[], "message")] as Argument<?>[]
        AnnotationValue<PulsarProducer> annotationValue = createAnnotation(false, false)

        when:
        ProducerBuilderImpl<?> builder = (ProducerBuilderImpl<?>) factory.createProducerBuilder(
                null,
                annotationValue,
                args,
                schemaHandler,
                "testMethod"
        )

        then:
        !builder.conf.batchingEnabled
        !builder.conf.blockIfQueueFull
    }

    void "test producer batching and blockQueue both enabled"() {
        given:
        TopicResolver topicResolver = { String topic -> topic } as TopicResolver
        DefaultSchemaHandler schemaHandler = new DefaultSchemaHandler(null)
        PulsarProducerFactory factory = new PulsarProducerFactory(topicResolver)
        Argument<?>[] args = [Argument.of(byte[], "message")] as Argument<?>[]
        AnnotationValue<PulsarProducer> annotationValue = createAnnotation(true, true)

        when:
        ProducerBuilderImpl<?> builder = (ProducerBuilderImpl<?>) factory.createProducerBuilder(
                null,
                annotationValue,
                args,
                schemaHandler,
                "testMethod"
        )

        then:
        builder.conf.batchingEnabled
        builder.conf.blockIfQueueFull
    }

    void "test producer chunking enabled and batching disabled"() {
        given:
        TopicResolver topicResolver = { String topic -> topic } as TopicResolver
        DefaultSchemaHandler schemaHandler = new DefaultSchemaHandler(null)
        PulsarProducerFactory factory = new PulsarProducerFactory(topicResolver)
        Argument<?>[] args = [Argument.of(byte[], "message")] as Argument<?>[]

        AnnotationValue<PulsarProducer> annotationValue = AnnotationValue.builder(PulsarProducer)
                .member("topic", "persistent://public/default/test-topic")
                .member("schema", MessageSchema.BYTES)
                .member("batching", false)
                .member("chunking", true)
                .build()

        when:
        ProducerBuilderImpl<?> builder = (ProducerBuilderImpl<?>) factory.createProducerBuilder(
                null,
                annotationValue,
                args,
                schemaHandler,
                "testMethod"
        )

        then:
        !builder.conf.batchingEnabled
        builder.conf.chunkingEnabled
    }
}
