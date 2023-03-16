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
package io.micronaut.pulsar.processor;

import io.micronaut.context.BeanContext;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.context.processor.ExecutableMethodProcessor;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.util.ArgumentUtils;
import io.micronaut.core.util.ArrayUtils;
import io.micronaut.core.util.StringUtils;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.messaging.exceptions.MessageListenerException;
import io.micronaut.pulsar.PulsarConsumerRegistry;
import io.micronaut.pulsar.annotation.PulsarConsumer;
import io.micronaut.pulsar.annotation.PulsarSubscription;
import io.micronaut.pulsar.config.DefaultPulsarClientConfiguration;
import io.micronaut.pulsar.events.ConsumerSubscribedEvent;
import io.micronaut.pulsar.events.ConsumerSubscriptionFailedEvent;
import jakarta.inject.Singleton;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.*;

/**
 * Processes beans containing methods annotated with @PulsarConsumer.
 *
 * @author Haris Secic
 * @since 1.0
 */
@Singleton
@Internal
public class PulsarConsumerProcessor implements ExecutableMethodProcessor<PulsarConsumer>, AutoCloseable,
    PulsarConsumerRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarConsumerProcessor.class);
    protected final TopicResolver topicResolver;
    protected final DefaultPulsarClientConfiguration pulsarClientConfiguration;

    private final ApplicationEventPublisher<Object> applicationEventPublisher;
    private final BeanContext beanContext;
    private final PulsarClient pulsarClient;
    private final DefaultSchemaHandler simpleSchemaResolver;
    private final Map<String, Consumer<?>> consumers = new ConcurrentHashMap<>();
    private final Map<String, Consumer<?>> paused = new ConcurrentHashMap<>();
    private final AtomicInteger consumerCounter = new AtomicInteger(10);

    public PulsarConsumerProcessor(final ApplicationEventPublisher<Object> applicationEventPublisher,
                                   final BeanContext beanContext,
                                   final PulsarClient pulsarClient,
                                   final DefaultSchemaHandler simpleSchemaResolver,
                                   final DefaultPulsarClientConfiguration pulsarClientConfiguration,
                                   final TopicResolver topicResolver) {
        this.applicationEventPublisher = applicationEventPublisher;
        this.beanContext = beanContext;
        this.pulsarClient = pulsarClient;
        this.simpleSchemaResolver = simpleSchemaResolver;
        this.pulsarClientConfiguration = pulsarClientConfiguration;
        this.topicResolver = topicResolver;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(final BeanDefinition<?> beanDefinition, final ExecutableMethod<?, ?> method) {
        final var consumerAnnotation = method.getDeclaredAnnotation(PulsarConsumer.class);
        if (null == consumerAnnotation) {
            return;
        }

        final var name = getConsumerName(consumerAnnotation);
        final var topicResolved = TopicResolver.extractTopic(consumerAnnotation,
            name,
            pulsarClientConfiguration.getShutdownOnSubscriberError());
        final var consumerId = topicResolver.generateIdFromMessagingClientName(name, topicResolved);

        if (consumers.containsKey(consumerId)) {
            throw new MessageListenerException(String.format("Consumer %s already exists", consumerId));
        }

        final var subscriptionAnnotation = method.getAnnotation(PulsarSubscription.class);
        final var arguments = method.getArguments();
        if (ArrayUtils.isEmpty(arguments)) {
            throw new MessageListenerException("Method annotated with PulsarConsumer must accept at least 1 parameter");
        }

        final var castMethod = (ExecutableMethod<Object, ?>) method;
        final var bean = beanContext.getBean(beanDefinition.getBeanType());

        final var consumerBuilder = processConsumerAnnotation(consumerAnnotation,
            subscriptionAnnotation,
            castMethod,
            bean,
            topicResolved);
        final boolean subscribeAsync = consumerAnnotation.getRequiredValue("subscribeAsync", Boolean.class);
        consumerBuilder.consumerName(name);
        if (subscribeAsync) {
            consumerBuilder.subscribeAsync().handle((consumer, ex) -> {
                if (null != ex) {
                    LOG.error("Failed subscribing Pulsar consumer {} {}", method.getDescription(false), consumerId, ex);
                    applicationEventPublisher.publishEventAsync(new ConsumerSubscriptionFailedEvent(ex, consumerId));
                    return new MessageListenerException("Failed to subscribe", ex);
                }
                consumers.put(consumerId, consumer);
                applicationEventPublisher.publishEventAsync(new ConsumerSubscribedEvent(consumer));
                if (pulsarClientConfiguration.getShutdownOnSubscriberError()) {
                    String msg = String.format("Failed to subscribe %s %s", consumerId, method.getDescription(false));
                    throw new Error(msg);
                }
                return consumer;
            });
        } else {
            try {
                final var consumer = consumerBuilder.subscribe();
                consumers.put(consumerId, consumer);
                applicationEventPublisher.publishEvent(new ConsumerSubscribedEvent(consumer));
            } catch (Exception e) {
                LOG.error("Failed subscribing Pulsar consumer {} {}", method.getDescription(false), consumerId, e);
                applicationEventPublisher.publishEvent(new ConsumerSubscriptionFailedEvent(e, consumerId));
                if (pulsarClientConfiguration.getShutdownOnSubscriberError()) {
                    final var msg = "Failed to subscribe %s %s with cause %s".formatted(name,
                        method.getDescription(false),
                        e.getMessage());
                    throw new Error(msg);
                }
                throw new MessageListenerException("Failed to subscribe %s".formatted(consumerId), e);
            }
        }
    }

    /**
     * Resolve topic name from the {@link PulsarConsumer} annotation.
     *
     * @param topic value of {@link PulsarConsumer} annotation
     * @return defined consumer name if set; otherwise generate a new one.
     */
    @NotNull
    protected String getConsumerName(final AnnotationValue<PulsarConsumer> topic) {
        return topic.stringValue("consumerName")
            .orElse("pulsar-consumer-" + consumerCounter.getAndIncrement());
    }

    @SuppressWarnings({"unchecked"})
    private ConsumerBuilder<?> processConsumerAnnotation(final AnnotationValue<PulsarConsumer> consumerAnnotation,
                                                         final AnnotationValue<PulsarSubscription> subscription,
                                                         //? will mess up IntelliJ and compiler so use Object to enable method.invoke
                                                         final ExecutableMethod<Object, ?> method,
                                                         final Object bean,
                                                         final TopicResolver.TopicResolved topic) {
        final var argHandler = new PulsarArgumentHandler(method.getArguments(), method.getDescription(false));
        final var schema = simpleSchemaResolver.decideSchema(argHandler.getBodyArgument(),
            argHandler.getKeyArgument(),
            consumerAnnotation,
            method.getDescription(false));

        final var consumer = new ConsumerBuilderImpl<>((PulsarClientImpl) pulsarClient, schema);
        consumerAnnotation.stringValue("consumerName").ifPresent(consumer::consumerName);

        resolveTopic(consumerAnnotation, consumer, topic);
        resolveDeadLetter(consumerAnnotation, consumer);

        if (null != subscription) {
            subscriptionValues(subscription, consumer);
        } else {
            consumerValues(consumerAnnotation, consumer);
        }

        consumerAnnotation.stringValue("ackTimeout").map(Duration::parse).ifPresent(duration -> {
            final long millis = duration.toMillis();
            if (1000 < millis) { // pulsar lib demands gt 1 second not gte
                consumer.ackTimeout(millis, MILLISECONDS);
            } else {
                throw new MessageListenerException("Acknowledge timeout must be greater than 1 second");
            }
        });

        consumer.messageListener(new DefaultListener(method, argHandler.isMessageWrapper(), bean, argHandler));

        return consumer;
    }

    private void resolveDeadLetter(AnnotationValue<PulsarConsumer> consumerAnnotation, ConsumerBuilder<?> consumerBuilder) {
        if (!this.pulsarClientConfiguration.getUseDeadLetterQueue()) {
            return;
        }
        final DeadLetterPolicy.DeadLetterPolicyBuilder builder = DeadLetterPolicy.builder();
        final Optional<String> deadLetterTopic = consumerAnnotation.stringValue("deadLetterTopic");
        if (deadLetterTopic.isPresent()) {
            final String topic = topicResolver.resolve(deadLetterTopic.get());
            builder.deadLetterTopic(topic);
        }
        final int maxRedeliverCount = consumerAnnotation.intValue("maxRetriesBeforeDlq")
            .orElse(pulsarClientConfiguration.getDefaultMaxRetryDlq());
        builder.maxRedeliverCount(maxRedeliverCount);
        consumerBuilder.deadLetterPolicy(builder.build());
    }

    private void resolveTopic(final AnnotationValue<PulsarConsumer> consumerAnnotation,
                              final ConsumerBuilder<?> consumer,
                              final TopicResolver.TopicResolved topic) {
        if (topic.isPattern()) {
            resolveTopicsPattern(consumerAnnotation, consumer, topicResolver.resolve(topic.getTopic()));
        } else if (topic.isArray()) {
            consumer.topic(Arrays.stream(topic.getTopics()).map(topicResolver::resolve).toArray(String[]::new));
        } else {
            consumer.topic(topicResolver.resolve(topic.getTopic()));
        }
    }

    private void resolveTopicsPattern(final AnnotationValue<PulsarConsumer> consumerAnnotation,
                                      final ConsumerBuilder<?> consumer,
                                      final String topicsPattern) {
        consumer.topicsPattern(topicsPattern);
        final var mode = consumerAnnotation.getRequiredValue(
            "subscriptionTopicsMode", RegexSubscriptionMode.class);
        consumer.subscriptionTopicsMode(mode);
        final var topicsRefresh = consumerAnnotation.intValue("patternAutoDiscoveryPeriod");
        if (topicsRefresh.isPresent()) {
            if (topicsRefresh.getAsInt() < 1) {
                throw new MessageListenerException("Topic " + topicsPattern + " refresh time cannot be below 1 second.");
            }
            consumer.patternAutoDiscoveryPeriod(topicsRefresh.getAsInt(), SECONDS);
        }
    }

    private void consumerValues(final AnnotationValue<PulsarConsumer> consumerAnnotation,
                                final ConsumerBuilder<?> consumer) {
        final var subscriptionName = consumerAnnotation.stringValue("subscription")
            .orElseGet(() -> "pulsar-subscription-" + consumerCounter.incrementAndGet());
        final var subscriptionType = consumerAnnotation.getRequiredValue(
            "subscriptionType", SubscriptionType.class);
        consumer.subscriptionName(subscriptionName).subscriptionType(subscriptionType);
    }

    private void subscriptionValues(final AnnotationValue<PulsarSubscription> subscription,
                                    final ConsumerBuilder<?> consumer) {
        final String subscriptionName = subscription.stringValue("subscriptionName")
            .orElse("pulsar-subscription-" + consumerCounter.incrementAndGet());

        consumer.subscriptionName(subscriptionName);

        subscription.enumValue("subscriptionType", SubscriptionType.class).ifPresent(consumer::subscriptionType);

        final var ackGroupTimeout = subscription.stringValue("ackGroupTimeout");
        if (ackGroupTimeout.isPresent()) {
            final var duration = Duration.parse(ackGroupTimeout.get());
            consumer.acknowledgmentGroupTime(duration.toNanos(), NANOSECONDS);
        }
    }

    @Override
    public void close() {
        for (Consumer<?> consumer : getConsumers().values()) {
            try {
                consumer.unsubscribe();
                consumer.close();
            } catch (Exception e) {
                LOG.warn("Error shutting down Pulsar consumer: {}", e.getMessage(), e);
            }
        }
    }

    @Override
    public Map<String, Consumer<?>> getConsumers() {
        return Collections.unmodifiableMap(consumers);
    }

    @NonNull
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> Consumer<T> getConsumer(@NonNull String id) {
        ArgumentUtils.requireNonNull("id", id);
        final Consumer consumer = consumers.get(id);
        if (consumer == null) {
            throw new IllegalArgumentException("No consumer found for ID: " + id);
        }
        return consumer;
    }

    @Override
    public boolean consumerExists(@NonNull String id) {
        return consumers.containsKey(id);
    }

    @NonNull
    @Override
    public Set<String> getConsumerIds() {
        return Collections.unmodifiableSet(consumers.keySet());
    }

    @Override
    public boolean isPaused(@NonNull String id) {
        if (StringUtils.isEmpty(id) || !consumers.containsKey(id)) {
            throw new IllegalArgumentException("No consumer found for ID: " + id);
        }
        return paused.containsKey(id);
    }

    @Override
    public void pause(@NonNull String id) {
        if (StringUtils.isEmpty(id) || !consumers.containsKey(id)) {
            throw new IllegalArgumentException("No consumer found for ID: " + id);
        }
        final Consumer<?> consumer = consumers.get(id);
        consumer.pause();
        paused.put(id, consumer);
    }

    @Override
    public void resume(@NonNull String id) {
        if (StringUtils.isEmpty(id) || !paused.containsKey(id)) {
            throw new IllegalArgumentException("No paused consumer found for ID: " + id);
        }
        final Consumer<?> consumer = paused.remove(id);
        consumer.resume();
    }
}
