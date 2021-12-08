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
package io.micronaut.pulsar.annotation;

import io.micronaut.aop.Around;
import io.micronaut.aop.Introduction;
import io.micronaut.context.annotation.AliasFor;
import io.micronaut.context.annotation.Type;
import io.micronaut.pulsar.MessageSchema;
import io.micronaut.pulsar.intercept.PulsarProducerAdvice;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.common.schema.KeyValueEncodingType;

import javax.validation.constraints.Pattern;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static io.micronaut.pulsar.MessageSchema.BYTES;
import static io.micronaut.pulsar.config.AbstractPulsarConfiguration.TOPIC_NAME_VALIDATOR;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.apache.pulsar.client.api.CompressionType.NONE;
import static org.apache.pulsar.client.api.HashingScheme.JavaStringHash;
import static org.apache.pulsar.client.api.MessageRoutingMode.RoundRobinPartition;

/**
 * Marks a method that should produce values to Pulsar topics on call.
 *
 * @author Haris Secic
 * @since 1.0
 */
@Documented
@Retention(RUNTIME)
@Target(METHOD)
@Around
@Introduction
@Type(PulsarProducerAdvice.class)
public @interface PulsarProducer {

    /**
     * @return Same as {@link #topic()}
     */
    @AliasFor(member = "topic")
    String value() default "";

    /**
     * @return Producer name.
     */
    String producerName() default "";

    /**
     * @return Topic to produce messages to
     */
    @AliasFor(member = "value")
    @Pattern(regexp = TOPIC_NAME_VALIDATOR)
    String topic() default "";

    /**
     * @return Type of message serialization.
     */
    MessageSchema schema() default BYTES;

    /**
     * @return Type of message key serialization.
     */
    MessageSchema keySchema() default BYTES;

    /**
     * If no {@link MessageKey} annotated method argument is detected this attribute is ignored and message is treated
     * as simple - without a key.
     *
     * @return Whether key will be in the message payload or separate.
     */
    KeyValueEncodingType keyEncoding() default KeyValueEncodingType.INLINE;

    /**
     * @return Compression type.
     */
    CompressionType compressionType() default NONE;

    /**
     * @return Message routing mode.
     */
    MessageRoutingMode messageRoutingMode() default RoundRobinPartition;

    /**
     * @return Produce messages of different schemas than specified at creation time
     */
    boolean multiSchema() default true;

    /**
     * @return Discover new partitions at runtime
     */
    boolean autoUpdatePartition() default true;

    /**
     * @return Multiple calls to send and sendAsync will block if queue full
     */
    boolean blockQueue() default false;

    /**
     * @return Enabled automatic batching of messages
     */
    boolean batching() default true;

    /**
     * @return Max messages in one batch
     */
    int batchingMaxMessages() default 1000;

    /**
     * Default 128KB.
     *
     * @return Max bytes per batch
     */
    int batchingMaxBytes() default 1024 * 128;

    /**
     * If this is enabled batching should be disabled.
     *
     * @return Split messages in chunks if bigger than max allowed size.
     */
    boolean chunking() default false;

    /**
     * @return Encryption key to add
     */
    String encryptionKey() default "";

    /**
     * @return Starting sequence ID for producer
     */
    long initialSequenceId() default Long.MIN_VALUE;

    /**
     * @return Change hashing scheme used to choose partition
     */
    HashingScheme hashingScheme() default JavaStringHash;

    /**
     * Defaults to false.
     * Will be used to determine whether to send message prior to executing method code or after.
     * @return Whether to send the message before calling actual implementation.
     */
    boolean sendBefore() default false;
}
