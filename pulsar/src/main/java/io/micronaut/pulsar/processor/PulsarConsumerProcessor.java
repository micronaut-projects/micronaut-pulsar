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
package io.micronaut.pulsar.processor;

import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.context.processor.ExecutableMethodProcessor;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.ArgumentUtils;
import io.micronaut.core.util.ArrayUtils;
import io.micronaut.core.util.StringUtils;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.pulsar.PulsarConsumerRegistry;
import io.micronaut.pulsar.annotation.PulsarConsumer;
import io.micronaut.pulsar.annotation.PulsarSubscription;
import io.micronaut.pulsar.config.DefaultPulsarClientConfiguration;
import io.micronaut.pulsar.events.ConsumerSubscribedEvent;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Processes beans containing methods annotated with @PulsarConsumer.
 *
 * @author Haris Secic
 * @since 1.0
 */
@Singleton
@Requires(beans = DefaultPulsarClientConfiguration.class)
public final class PulsarConsumerProcessor implements ExecutableMethodProcessor<PulsarConsumer>, AutoCloseable,
        PulsarConsumerRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarConsumerProcessor.class);

    private final ApplicationEventPublisher applicationEventPublisher;
    private final BeanContext beanContext;
    private final PulsarClient pulsarClient;
    private final SchemaResolver schemaResolver;
    private final Map<String, Consumer<?>> consumers = new ConcurrentHashMap<>();
    private final Map<String, Consumer<?>> paused = new ConcurrentHashMap<>();
    private final AtomicInteger consumerCounter = new AtomicInteger(10);

    public PulsarConsumerProcessor(ApplicationEventPublisher applicationEventPublisher,
                                   BeanContext beanContext,
                                   PulsarClient pulsarClient,
                                   SchemaResolver schemaResolver) {
        this.applicationEventPublisher = applicationEventPublisher;
        this.beanContext = beanContext;
        this.pulsarClient = pulsarClient;
        this.schemaResolver = schemaResolver;
    }

    @Override
    public void process(BeanDefinition<?> beanDefinition, ExecutableMethod<?, ?> method) {
        List<AnnotationValue<PulsarConsumer>> topicAnnotation = method.getAnnotationValuesByType(PulsarConsumer.class);
        AnnotationValue<PulsarSubscription> subscriptionAnnotation = method.getAnnotation(PulsarSubscription.class);

        if (topicAnnotation.isEmpty()) {
            return; //probably never happens
        }

        Argument<?>[] arguments = method.getArguments();

        if (ArrayUtils.isEmpty(arguments)) {
            throw new IllegalArgumentException("Method annotated with PulsarConsumer must accept at least 1 parameter");
        } else if (arguments.length > 2) {
            throw new IllegalArgumentException("Method annotated with PulsarConsumer must accept maximum of 2 parameters " +
                    "one for message body and one for " + Consumer.class.getName());
        }

        //because of processTopic requirements
        @SuppressWarnings("unchecked")
        ExecutableMethod<Object, ?> castMethod = (ExecutableMethod<Object, ?>) method;

        Object bean = beanContext.getBean(beanDefinition.getBeanType());

        for (AnnotationValue<PulsarConsumer> topic : topicAnnotation) {
            ConsumerBuilder<?> consumerBuilder = processTopic(topic, subscriptionAnnotation, castMethod, bean);
            boolean subscribeAsync = topic.getRequiredValue("subscribeAsync", Boolean.class);
            String name = topic.stringValue("consumerName")
                    .orElseGet(() -> "pulsar-consumer-" + consumerCounter.get());
            consumerBuilder.consumerName(name);
            if (subscribeAsync) {
                consumerBuilder.subscribeAsync().thenAccept(x -> {
                    consumers.put(name, x);
                    applicationEventPublisher.publishEventAsync(new ConsumerSubscribedEvent(x));
                });
            } else {
                try {
                    Consumer<?> consumer = consumerBuilder.subscribe();
                    consumers.put(name, consumer);
                    applicationEventPublisher.publishEvent(new ConsumerSubscribedEvent(consumer));
                } catch (PulsarClientException e) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Could not start pulsar producer " + name, e);
                    }
                }
            }
        }
    }

    @SuppressWarnings({"unchecked"})
    private ConsumerBuilder<?> processTopic(AnnotationValue<PulsarConsumer> consumerAnnotation,
                                            AnnotationValue<PulsarSubscription> subscription,
                                            //? will mess up IntelliJ and compiler so use Object to enable method.invoke
                                            ExecutableMethod<Object, ?> method,
                                            Object bean) {

        Argument<?>[] methodArguments = method.getArguments();
        Class<?> messageBodyType;
        Optional<Argument<?>> bodyType = Arrays.stream(methodArguments).filter(x -> !Consumer.class.isAssignableFrom(x.getType())).findFirst();

        if (!bodyType.isPresent()) {
            throw new IllegalArgumentException("Method annotated with pulsar consumer must accept 1 parameter that's of" +
                    " type other than " + Consumer.class.getName() + " class which can be used to accept pulsar message.");
        }

        messageBodyType = bodyType.get().getType();
        boolean useMessageWrapper = Message.class.isAssignableFrom(messageBodyType); //users can consume only message body and ignore the rest

        Schema<?> schema;
        if (useMessageWrapper) {
            Argument<?> messageWrapper = bodyType.get().getTypeParameters()[0];
            schema = schemaResolver.decideSchema(consumerAnnotation, messageWrapper.getType());
        } else {
            schema = schemaResolver.decideSchema(consumerAnnotation, messageBodyType);
        }

        ConsumerBuilder<?> consumer = pulsarClient.newConsumer(schema);
        consumerAnnotation.stringValue("consumerName").ifPresent(consumer::consumerName);

        resolveTopic(consumerAnnotation, consumer);

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
                throw new IllegalArgumentException("Acknowledge timeout must be greater than 1 second");
            }
        });

        int consumerIndex = IntStream.range(0, methodArguments.length)
                .filter(i -> Consumer.class.isAssignableFrom(methodArguments[i].getType()))
                .findFirst().orElse(-1);

        consumer.messageListener(new DefaultListener(method, useMessageWrapper, consumerIndex, bean));

        return consumer;
    }

    private void resolveTopic(AnnotationValue<PulsarConsumer> consumerAnnotation, ConsumerBuilder<?> consumer) {
        String topic = consumerAnnotation.stringValue().orElse(null);
        String[] topics = consumerAnnotation.stringValues("topics");
        String topicsPattern = consumerAnnotation.stringValue("topicsPattern").orElse(null);

        if (StringUtils.isNotEmpty(topic)) {
            consumer.topic(topic);
        } else if (ArrayUtils.isNotEmpty(topics)) {
            consumer.topic(topics);
        } else if (StringUtils.isNotEmpty(topicsPattern)) {
            resolveTopicsPattern(consumerAnnotation, consumer, topicsPattern);
        } else {
            throw new IllegalArgumentException("Pulsar consumer requires topics or topicsPattern value");
        }
    }

    private void resolveTopicsPattern(AnnotationValue<PulsarConsumer> consumerAnnotation, ConsumerBuilder<?> consumer, String topicsPattern) {
        consumer.topicsPattern(topicsPattern);
        RegexSubscriptionMode mode = consumerAnnotation.getRequiredValue("subscriptionTopicsMode", RegexSubscriptionMode.class);
        consumer.subscriptionTopicsMode(mode);
        OptionalInt topicsRefresh = consumerAnnotation.intValue("patternAutoDiscoveryPeriod");
        if (topicsRefresh.isPresent()) {
            if (topicsRefresh.getAsInt() < 1) {
                throw new IllegalArgumentException("Topic " + topicsPattern + " refresh time cannot be below 1 second.");
            }
            consumer.patternAutoDiscoveryPeriod(topicsRefresh.getAsInt(), SECONDS);
        }
    }

    private void consumerValues(AnnotationValue<PulsarConsumer> consumerAnnotation, ConsumerBuilder<?> consumer) {
        String subscriptionName = consumerAnnotation.stringValue("subscription")
                .orElseGet(() -> "pulsar-subscription-" + consumerCounter.incrementAndGet());
        SubscriptionType subscriptionType = consumerAnnotation.getRequiredValue("subscription", SubscriptionType.class);
        consumer.subscriptionName(subscriptionName).subscriptionType(subscriptionType);
    }

    private void subscriptionValues(AnnotationValue<PulsarSubscription> subscription, ConsumerBuilder<?> consumer) {
        String subscriptionName = subscription.stringValue("subscriptionName")
                .orElse("pulsar-subscription-" + consumerCounter.incrementAndGet());

        consumer.subscriptionName(subscriptionName);

        subscription.enumValue("subscriptionType", SubscriptionType.class)
                .ifPresent(consumer::subscriptionType);

        Optional<String> ackGroupTimeout = subscription.stringValue("ackGroupTimeout");
        if (ackGroupTimeout.isPresent()) {
            Duration duration = Duration.parse(ackGroupTimeout.get());
            consumer.acknowledgmentGroupTime(duration.toNanos(), NANOSECONDS);
        }
    }

    @Override
    public void close() throws PulsarClientException {
        for (Consumer<?> consumer : getConsumers().values()) {
            consumer.unsubscribe();
            consumer.close();
        }
    }

    @Override
    public Map<String, Consumer<?>> getConsumers() {
        return Collections.unmodifiableMap(consumers);
    }

    @Nonnull
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> Consumer<T> getConsumer(@Nonnull String id) {
        ArgumentUtils.requireNonNull("id", id);
        final Consumer consumer = consumers.get(id);
        if (consumer == null) {
            throw new IllegalArgumentException("No consumer found for ID: " + id);
        }
        return consumer;
    }

    @Nonnull
    @Override
    public Set<String> getConsumerIds() {
        return Collections.unmodifiableSet(consumers.keySet());
    }

    @Override
    public boolean isPaused(@Nonnull String id) {
        if (StringUtils.isNotEmpty(id) && consumers.containsKey(id)) {
            return paused.containsKey(id);
        } else {
            throw new IllegalArgumentException("No consumer found for ID: " + id);
        }
    }

    @Override
    public void pause(@Nonnull String id) {
        if (StringUtils.isEmpty(id) || !consumers.containsKey(id)) {
            throw new IllegalArgumentException("No consumer found for ID: " + id);
        }
        Consumer<?> consumer = consumers.get(id);
        consumer.pause();
        paused.put(id, consumer);
    }

    @Override
    public void resume(@Nonnull String id) {
        if (StringUtils.isNotEmpty(id) && paused.containsKey(id)) {
            Consumer<?> consumer = paused.remove(id);
            consumer.resume();
        } else {
            throw new IllegalArgumentException("No paused consumer found for ID: " + id);
        }
    }
}
