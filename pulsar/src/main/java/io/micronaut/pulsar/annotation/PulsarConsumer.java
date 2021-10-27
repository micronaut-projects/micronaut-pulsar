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

import io.micronaut.context.annotation.AliasFor;
import io.micronaut.context.annotation.Executable;
import io.micronaut.messaging.annotation.MessageListener;
import io.micronaut.messaging.annotation.MessageMapping;
import io.micronaut.pulsar.MessageSchema;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;

import javax.validation.constraints.Pattern;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static io.micronaut.pulsar.MessageSchema.BYTES;
import static io.micronaut.pulsar.config.AbstractPulsarConfiguration.TOPIC_NAME_PATTERN_VALIDATOR;
import static io.micronaut.pulsar.config.AbstractPulsarConfiguration.TOPIC_NAME_VALIDATOR;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.apache.pulsar.client.api.RegexSubscriptionMode.AllTopics;
import static org.apache.pulsar.client.api.SubscriptionType.Exclusive;

/**
 * Marks a method as a Pulsar Consumer.
 *
 * @author Haris Secic
 * @since 1.0
 */
@Documented
@Retention(RUNTIME)
@Target(METHOD)
@Executable
@MessageListener
public @interface PulsarConsumer {

    /**
     * @return Same as {@link #topic()}
     */
    @AliasFor(member = "topic")
    @Pattern(regexp = TOPIC_NAME_VALIDATOR)
    @AliasFor(annotation = MessageMapping.class, member = "value")
    String value() default "";

    /**
     * Has precedence over {@link #topics()} and {@link #topicsPattern()}.
     * Single topic to consume messages from.
     *
     * @return Topic name to listen to
     */
    @Pattern(regexp = TOPIC_NAME_VALIDATOR)
    @AliasFor(member = "value")
    @AliasFor(annotation = MessageMapping.class, member = "value")
    String topic() default "";

    /**
     * Has precedence over {@code topicPattern}.
     *
     * @return List of topic names in form of (persistent|non-persistent)://tenant-name/namespace/topic.
     */
    @Pattern(regexp = TOPIC_NAME_VALIDATOR)
    @AliasFor(annotation = MessageMapping.class, member = "value")
    String[] topics() default {};

    /**
     * Ignored if {@code topics} attribute is set.
     *
     * @return Topics name in form of tenantName/namespace/topic-name-pattern.
     */
    @Pattern(regexp = TOPIC_NAME_PATTERN_VALIDATOR)
    @AliasFor(annotation = MessageMapping.class, member = "value")
    String topicsPattern() default "";

    /**
     * Defaults to {@link MessageSchema#BYTES} as default value for Pulsar {@link org.apache.pulsar.client.api.Schema} is {@code byte[]}.
     *
     * @return Schema to use with pulsar topic consumer
     */
    MessageSchema schema() default BYTES;

    /**
     * @return Consumer name for more descriptive monitoring
     */
    String consumerName();

    /**
     * @return Subscription name in case consumer was defined outside the {@link PulsarSubscription} annotated class
     */
    String subscription() default "";

    /**
     * @return Subscription type in case consumer was defined outside of {@link PulsarSubscription} annotated class
     */
    SubscriptionType subscriptionType() default Exclusive;

    /**
     * Ignored if {@code topics()} attribute is set.
     * Default value {@link RegexSubscriptionMode#AllTopics}
     * <p>
     * Whether to read topics from persistent, or non-persistent storage, or both.
     * <p>
     *
     * @return subscription
     */
    RegexSubscriptionMode subscriptionTopicsMode() default AllTopics;

    /**
     * Used in combination with {@link #topicsPattern()}. Ignored using {@link #topics()}. Must be greater than
     * 1. Low values should be avoided. Pulsar default value is 1 minute
     *
     * @return Amount of delay between checks, in seconds, for new topic matching given pattern.
     */
    int patternAutoDiscoveryPeriod() default -1;

    /**
     * By default, consumer should subscribe in non-blocking manner using default {@link java.util.concurrent.CompletableFuture}
     * of {@link org.apache.pulsar.client.api.ConsumerBuilder#subscribeAsync()}.
     * <p>
     * If blocking set to false application will block until consumer is successfully subscribed. This is different
     * from the actual process of consuming the messages which still happens in separate thread managed by the
     * underlying Pulsar Client library.
     *
     * @return Should the consumer subscribe in async manner or blocking
     */
    boolean subscribeAsync() default true;

    /**
     * By default, it will use default value of {@link org.apache.pulsar.client.api.ConsumerBuilder} which is disabled
     * and no redelivery happens unless consumer crashed.
     * <p>
     * Must be greater than 1s.
     *
     * @return Allowed time to pass before message is acknowledged.
     * @see org.apache.pulsar.client.api.ConsumerBuilder#ackTimeout
     */
    String ackTimeout() default "";

    /**
     * @return Number of items allowed in the queue. Default 1000 as in Pulsar Java Client
     */
    int receiverQueueSize() default 1000;

    /**
     * By default no priority is set.
     * Use any value less than 0 to disable. Use anything above 0 to set lower priority level.
     *
     * @return priority level for a consumer
     * @see org.apache.pulsar.client.api.ConsumerBuilder#priorityLevel(int)
     */
    int priorityLevel() default -1;

    /**
     * Default is fallback to Pulsar Client Java library.
     *
     * @return Dead Letter topic name.
     */
    String deadLetterTopic() default "";

    /**
     * @return Maximum numbers of retires before sending message to dead letter queue topic.
     */
    int maxRetriesBeforeDlq() default 3;
}
