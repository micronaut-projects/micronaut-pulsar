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
package io.micronaut.pulsar.annotation;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Marks a method that should produce values to pulsar topics on call.
 *
 * @author Haris Secic
 * @since 1.0
 */
@Documented
@Retention(RUNTIME)
@Target({ElementType.METHOD})
public @interface PulsarProducer {

    /**
     * @return Producer name.
     */
    String producerName();

    /**
     * @return Topic to produce messages to
     */
    String topic();

    /**
     * @return Type of message serialization.
     */
    MessageSchema schema() default MessageSchema.BYTES;

    /**
     * @return Class to expect as message body.
     */
    Class<?> bodyType() default byte[].class;

    /**
     * @return Compression type.
     */
    CompressionType compressionType() default CompressionType.NONE;

    /**
     * @return Message routing mode.
     */
    MessageRoutingMode messageRoutingMode() default MessageRoutingMode.RoundRobinPartition;

    /**
     * @return Produce messages of different schemas than one specified at creation time
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
     * Default 128KB
     *
     * @return Max bytes per batch
     */
    int batchingMaxBytes() default 1000 * 128;

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
    HashingScheme hashingScheme() default HashingScheme.JavaStringHash;
}
