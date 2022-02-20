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
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.context.processor.ExecutableMethodProcessor;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.type.Argument;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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

    private final ApplicationEventPublisher<Object> applicationEventPublisher;
    private final BeanContext beanContext;
    private final PulsarClient pulsarClient;
    private final DefaultSchemaHandler simpleSchemaResolver;
    private final DefaultPulsarClientConfiguration pulsarClientConfiguration;
    private final Map<String, Consumer<?>> consumers = new ConcurrentHashMap<>();
    private final Map<String, Consumer<?>> paused = new ConcurrentHashMap<>();
    private final AtomicInteger consumerCounter = new AtomicInteger(10);
    protected final TopicResolver topicResolver;

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
        final AnnotationValue<PulsarConsumer> topic = method.getDeclaredAnnotation(PulsarConsumer.class);
        if (null == topic) {
            return;
        }

        final String name = getConsumerName(topic);
        if (consumers.containsKey(name)) {
            throw new MessageListenerException(String.format("Consumer %s already exists", name));
        }

        final AnnotationValue<PulsarSubscription> subscriptionAnnotation = method.getAnnotation(PulsarSubscription.class);
        final Argument<?>[] arguments = method.getArguments();
        if (ArrayUtils.isEmpty(arguments)) {
            throw new MessageListenerException("Method annotated with PulsarConsumer must accept at least 1 parameter");
        }

        final ExecutableMethod<Object, ?> castMethod = (ExecutableMethod<Object, ?>) method;
        final Object bean = beanContext.getBean(beanDefinition.getBeanType());

        final ConsumerBuilder<?> consumerBuilder = processConsumerAnnotation(topic, subscriptionAnnotation, castMethod, bean);
        final boolean subscribeAsync = topic.getRequiredValue("subscribeAsync", Boolean.class);
        consumerBuilder.consumerName(name);
        if (subscribeAsync) {
            consumerBuilder.subscribeAsync().handle((consumer, ex) -> {
                if (null != ex) {
                    LOG.error("Failed subscribing Pulsar consumer {} {}", method.getDescription(false), name, ex);
                    applicationEventPublisher.publishEventAsync(new ConsumerSubscriptionFailedEvent(ex, name));
                    return new MessageListenerException("Failed to subscribe", ex);
                }
                consumers.put(name, consumer);
                applicationEventPublisher.publishEventAsync(new ConsumerSubscribedEvent(consumer));
                if (pulsarClientConfiguration.getShutdownOnSubscriberError()) {
                    String msg = String.format("Failed to subscribe %s %s", name, method.getDescription(false));
                    throw new Error(msg);
                }
                return consumer;
            });
        } else {
            try {
                final Consumer<?> consumer = consumerBuilder.subscribe();
                consumers.put(name, consumer);
                applicationEventPublisher.publishEvent(new ConsumerSubscribedEvent(consumer));
            } catch (Exception e) {
                LOG.error("Failed subscribing Pulsar consumer {} {}", method.getDescription(false), name, e);
                applicationEventPublisher.publishEvent(new ConsumerSubscriptionFailedEvent(e, name));
                if (pulsarClientConfiguration.getShutdownOnSubscriberError()) {
                    final String msg = String.format("Failed to subscribe %s %s with cause %s",
                            name,
                            method.getDescription(false),
                            e.getMessage());
                    throw new Error(msg);
                }
                final String message = String.format("Failed to subscribe %s", name);
                throw new MessageListenerException(message, e);
            }
        }
    }

    protected String getConsumerName(final AnnotationValue<PulsarConsumer> topic) {
        return topic.stringValue("consumerName").orElse("pulsar-consumer-" + consumerCounter.getAndIncrement());
    }

    @SuppressWarnings({"unchecked"})
    private ConsumerBuilder<?> processConsumerAnnotation(AnnotationValue<PulsarConsumer> consumerAnnotation,
                                                         AnnotationValue<PulsarSubscription> subscription,
                                                         //? will mess up IntelliJ and compiler so use Object to enable method.invoke
                                                         ExecutableMethod<Object, ?> method,
                                                         Object bean) {
        final PulsarArgumentHandler argHandler = new PulsarArgumentHandler(method.getArguments(), method.getDescription(false));
        final Schema<?> schema = simpleSchemaResolver.decideSchema(argHandler.getBodyArgument(),
                argHandler.getKeyArgument(),
                consumerAnnotation,
                method.getDescription(false));

        final ConsumerBuilder<?> consumer = new ConsumerBuilderImpl<>((PulsarClientImpl) pulsarClient, schema);
        consumerAnnotation.stringValue("consumerName").ifPresent(consumer::consumerName);

        resolveTopic(consumerAnnotation, consumer);
        resolveDeadLetter(consumerAnnotation, consumer);

        if (null != subscription) {
            subscriptionValues(subscription, consumer);
        } else {
            consumerValues(consumerAnnotation, consumer);
        }

        consumerAnnotation.stringValue("ackTimeout").map(Duration::parse).ifPresent(duration -> {
            long millis = duration.toMillis();
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
        Boolean useDeadLetterQueue = this.pulsarClientConfiguration.getUseDeadLetterQueue();
        if (!useDeadLetterQueue) {
            return;
        }
        DeadLetterPolicy.DeadLetterPolicyBuilder builder = DeadLetterPolicy.builder();
        consumerAnnotation.stringValue("deadLetterTopic").ifPresent(builder::deadLetterTopic);
        int maxRedeliverCount = consumerAnnotation.intValue("maxRetriesBeforeDlq").orElse(pulsarClientConfiguration.getDefaultMaxRetryDlq());
        builder.maxRedeliverCount(maxRedeliverCount);
        consumerBuilder.deadLetterPolicy(builder.build());
    }

    private void resolveTopic(AnnotationValue<PulsarConsumer> consumerAnnotation, ConsumerBuilder<?> consumer) {
        final TopicResolved topic = extractTopic(consumerAnnotation);
        if (topic.isPattern()) {
            resolveTopicsPattern(consumerAnnotation, consumer, topicResolver.resolve(topic.getTopic()));
        }
        if (topic.isArray()) {
            consumer.topic(Arrays.stream(topic.getTopics()).map(topicResolver::resolve).toArray(String[]::new));
        }
        consumer.topic(topicResolver.resolve(topic.getTopic()));
    }

    @NonNull
    protected TopicResolved extractTopic(final AnnotationValue<PulsarConsumer> consumerAnnotation) {
        final String topic = consumerAnnotation.stringValue().orElse(null);
        if (StringUtils.isNotEmpty(topic)) {
            return new TopicResolved(topic, false);
        }
        final String[] topics = consumerAnnotation.stringValues("topics");
        if (ArrayUtils.isNotEmpty(topics)) {
            return new TopicResolved(topics, false);
        }
        final String topicsPattern = consumerAnnotation.stringValue("topicsPattern").orElse(null);
        if (StringUtils.isNotEmpty(topicsPattern)) {
            return new TopicResolved(topicsPattern, true);
        }
        throw new MessageListenerException("Pulsar consumer requires topics or topicsPattern value");
    }

    static class TopicResolved {
        private final Object value;
        private final boolean isPattern;

        TopicResolved(Object value, boolean isPattern) {
            this.value = value;
            this.isPattern = isPattern;
        }

        public String getTopic() {
            return (String) value;
        }

        public String[] getTopics() {
            return (String[]) value;
        }

        public boolean isPattern() {
            return isPattern;
        }

        public boolean isArray() {
            return value instanceof String[];
        }

        public boolean isDynamicTenant() {
            if (isArray()) {
                return Arrays.stream(getTopics()).anyMatch(TopicResolver::isDynamicTenantInTopic);
            }
            return TopicResolver.isDynamicTenantInTopic(getTopic());
        }

        public String getId() {
            if (!isArray()) {
                return getTopic();
            }
            return String.join(";", getTopics());
        }
    }

    private void resolveTopicsPattern(final AnnotationValue<PulsarConsumer> consumerAnnotation,
                                      final ConsumerBuilder<?> consumer,
                                      final String topicsPattern) {
        consumer.topicsPattern(topicsPattern);
        final RegexSubscriptionMode mode = consumerAnnotation.getRequiredValue(
                "subscriptionTopicsMode", RegexSubscriptionMode.class);
        consumer.subscriptionTopicsMode(mode);
        final OptionalInt topicsRefresh = consumerAnnotation.intValue("patternAutoDiscoveryPeriod");
        if (topicsRefresh.isPresent()) {
            if (topicsRefresh.getAsInt() < 1) {
                throw new MessageListenerException("Topic " + topicsPattern + " refresh time cannot be below 1 second.");
            }
            consumer.patternAutoDiscoveryPeriod(topicsRefresh.getAsInt(), SECONDS);
        }
    }

    private void consumerValues(final AnnotationValue<PulsarConsumer> consumerAnnotation,
                                final ConsumerBuilder<?> consumer) {
        String subscriptionName = consumerAnnotation.stringValue("subscription")
                .orElseGet(() -> "pulsar-subscription-" + consumerCounter.incrementAndGet());
        SubscriptionType subscriptionType = consumerAnnotation.getRequiredValue(
                "subscriptionType", SubscriptionType.class);
        consumer.subscriptionName(subscriptionName).subscriptionType(subscriptionType);
    }

    private void subscriptionValues(final AnnotationValue<PulsarSubscription> subscription,
                                    final ConsumerBuilder<?> consumer) {
        final String subscriptionName = subscription.stringValue("subscriptionName")
                .orElse("pulsar-subscription-" + consumerCounter.incrementAndGet());

        consumer.subscriptionName(subscriptionName);

        subscription.enumValue("subscriptionType", SubscriptionType.class).ifPresent(consumer::subscriptionType);

        final Optional<String> ackGroupTimeout = subscription.stringValue("ackGroupTimeout");
        if (ackGroupTimeout.isPresent()) {
            final Duration duration = Duration.parse(ackGroupTimeout.get());
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
        Consumer<?> consumer = consumers.get(id);
        consumer.pause();
        paused.put(id, consumer);
    }

    @Override
    public void resume(@NonNull String id) {
        if (StringUtils.isEmpty(id) || !paused.containsKey(id)) {
            throw new IllegalArgumentException("No paused consumer found for ID: " + id);
        }
        Consumer<?> consumer = paused.remove(id);
        consumer.resume();
    }
}
