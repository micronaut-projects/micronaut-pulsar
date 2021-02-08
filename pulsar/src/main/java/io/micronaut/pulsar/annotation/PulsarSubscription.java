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

import io.micronaut.messaging.annotation.MessageListener;
import org.apache.pulsar.client.api.SubscriptionType;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.apache.pulsar.client.api.SubscriptionType.Exclusive;

/**
 * Mark a class that contains Pulsar consumers. Each method in class should be
 * isolated consumer. However Pulsar provides multiple consumers via single
 * subscription if they are set to Failover, Share, or such.
 *
 * @author Haris Secic
 * @since 1.0
 */
@Documented
@Retention(RUNTIME)
@Target(TYPE)
@MessageListener
public @interface PulsarSubscription {

    /**
     * If not set, UUID will be generated as subscription name to avoid
     * collisions if consumer type is Exclusive.
     *
     * @return Subscription name
     * @see org.apache.pulsar.client.api.ConsumerBuilder#subscriptionType
     */
    String subscriptionName() default "";

    /**
     * By default {@code Exclusive}.
     *
     * @return Type of consumer subscription
     * @see org.apache.pulsar.client.api.ConsumerBuilder#subscriptionType
     */
    SubscriptionType subscriptionType() default Exclusive;

    /**
     * By default it will use PulsarConsumer builder default values.
     *
     * @return Maximum amount of time allowed to pass for message to be
     * acknowledged or else redelivery happens.
     * @see org.apache.pulsar.client.api.ConsumerBuilder#acknowledgmentGroupTime
     */
    String ackGroupTimeout() default "";
}
