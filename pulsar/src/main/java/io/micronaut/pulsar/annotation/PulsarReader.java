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

import io.micronaut.pulsar.MessageSchema;
import io.micronaut.pulsar.config.AbstractPulsarConfiguration;
import org.apache.pulsar.client.api.RegexSubscriptionMode;

import javax.validation.constraints.Pattern;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Create and inject Pulsar reader into field.
 *
 * @author Haris Secic
 * @since 1.0
 */
@Documented
@Retention(RUNTIME)
@Target({ElementType.FIELD})
public @interface PulsarReader {

    /**
     * @return List of topic names in form of (persistent|non-persistent)://tenant-name/namespace/topic
     */
    @Pattern(regexp = AbstractPulsarConfiguration.TOPIC_VALIDATOR)
    String[] topics() default {};

    /**
     * Ignored if {@code topics()} attribute is set.
     * @return Topics name in form of tenantName/namespace/topic-name-pattern.
     */
    @Pattern(regexp = AbstractPulsarConfiguration.TOPIC_NAME_VALIDATOR)
    String topicsPattern() default "";

    /**
     * Defaults to {@code byte[]}.
     *
     * @return Type of body in puslar message
     */
    Class<?> messageBodyType() default byte[].class;

    /**
     * Defaults to {@link MessageSchema#BYTES} as default value for Pulsar {@link org.apache.pulsar.client.api.Schema} is {@code byte[]}.
     *
     * @return Schema to use with pulsar topic consumer
     */
    MessageSchema schema() default MessageSchema.BYTES;

    /**
     * @return Consumer name for more descriptive monitoring
     */
    String consumerName();

    /**
     * Default value {@link RegexSubscriptionMode#AllTopics}
     * <p>
     * Whether to read topics from persistent, or non-persistent storage, or both
     * <p>
     * If topics are set other than {@link this#topicsPattern()} this value will be ignored.
     *
     * @return subscription
     */
    RegexSubscriptionMode subscriptionTopicsMode();

    /**
     * Used in combination with {@link this#topicsPattern()}. Ignored using {@link this#topics()}. Must be greater than
     * 1. Low values should be avoided as it will use network too much and could lead to false DDOS attack detection.
     *
     * @return Amount of delay between checks, in seconds, for new topic matching given pattern.
     */
    int patternAutoDiscoveryPeriod() default -1;

    /**
     * By default consumer should subscribe in non-blocking manner using default {@link java.util.concurrent.CompletableFuture} of {@link org.apache.pulsar.client.api.ConsumerBuilder#subscribeAsync()}.
     * <p>
     * If blocking set to false application will block until consumer is successfully subscribed
     *
     * @return Should the consumer subscribe in async manner or blocking
     */
    boolean subscribeAsync() default true;

    /**
     * By default it will use default value of {@link org.apache.pulsar.client.api.ConsumerBuilder} which is disabled
     * and no redelivery happens unless consumer crashed.
     * <p>
     * Must be greater than 1s.
     *
     * @return Allowed time to pass before message is acknowledged.
     * @see org.apache.pulsar.client.api.ConsumerBuilder#ackTimeout
     */
    String ackTimeout() default "";

    /**
     * @return Number of items allowed in the queue. Default -1 as in Pulsar Java Client
     */
    int receiverQueueSize() default -1;

    /**
     * By default no priority is set.
     * Use any value less than 0 to disable. Use anything above 0 to set lower priority level.
     *
     * @return priority level for a consumer
     * @see org.apache.pulsar.client.api.ConsumerBuilder#priorityLevel(int)
     */
    int priorityLevel() default -1;
}
