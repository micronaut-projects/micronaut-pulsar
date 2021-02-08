/*
 * Copyright 2021 original authors
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
package io.micronaut.pulsar.annotation;

import io.micronaut.context.annotation.AliasFor;
import io.micronaut.pulsar.MessageSchema;

import javax.validation.constraints.Pattern;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static io.micronaut.pulsar.MessageSchema.BYTES;
import static io.micronaut.pulsar.config.AbstractPulsarConfiguration.TOPIC_NAME_VALIDATOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Create and inject Pulsar reader into field.
 *
 * @author Haris Secic
 * @since 1.0
 */
@Documented
@Retention(RUNTIME)
@Target({PARAMETER, FIELD})
public @interface PulsarReader {

    /**
     * @return topic name to listen to
     * @see #topic()
     */
    @AliasFor(member = "topic")
    String value() default "";

    /**
     * Only single topic subscription possible for readers.
     *
     * @return topic name to listen to
     */
    @AliasFor(member = "value")
    @Pattern(regexp = TOPIC_NAME_VALIDATOR)
    String topic() default "";

    /**
     * Defaults to {@link MessageSchema#BYTES} as default value for Pulsar {@link org.apache.pulsar.client.api.Schema} is {@code byte[]}.
     *
     * @return Schema to use with pulsar topic consumer
     */
    MessageSchema schema() default BYTES;

    /**
     * @return Consumer name.
     */
    String readerName();

    /**
     * By default reader should subscribe in non-blocking manner using default {@link java.util.concurrent.CompletableFuture} of {@link org.apache.pulsar.client.api.ConsumerBuilder#subscribeAsync()}.
     * <p>
     * If blocking set to false application will block until consumer is successfully subscribed
     *
     * @return Should the consumer subscribe in async manner or blocking
     */
    boolean subscribeAsync() default true;

    /**
     * @return Whether to position reader to newest available message in queue or not.
     */
    boolean startMessageLatest() default true;
}
