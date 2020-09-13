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
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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
                consumerBuilder.subscribeAsync().thenAccept(x -> consumers.put(name, x));
            } else {
                try {
                    Consumer<?> consumer = consumerBuilder.subscribe();
                    consumers.put(name, consumer);
                    applicationEventPublisher.publishEvent(new ConsumerSubscribedEvent(consumer));
                } catch (PulsarClientException e) {
                    LOG.error("Could not start pulsar producer " + name, e);
                }
            }
        }
    }

    @SuppressWarnings({"unchecked"})
    private ConsumerBuilder<?> processTopic(AnnotationValue<PulsarConsumer> topicAnnotation,
                                            AnnotationValue<PulsarSubscription> listener,
                                            //? will mess up IntelliJ and compiler so use Object to enable method.invoke
                                            ExecutableMethod<Object, ?> method,
                                            Object bean) {

        Argument<?>[] methodArguments = method.getArguments();
        Class<?> messageBodyType = topicAnnotation.classValue("messageBodyType")
                .orElse(topicAnnotation.getRequiredValue("messageBodyType", Class.class));
        Stream<Argument<?>> argumentsStream = Arrays.stream(methodArguments);

        Argument<?> messageListener = argumentsStream
                .filter(x -> x.getType() == messageBodyType || Message.class.isAssignableFrom(x.getType()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Method annotated with pulsar consumer must accept " +
                        "parameter of type T or Message<T> where T is defined in PulsarConsumer.messageBodyType"));

        boolean useMessageWrapper = false; //users can consume only message body and ignore the rest

        if (Message.class.isAssignableFrom(messageListener.getType())) {
            Argument<?> messageParameter = messageListener.getFirstTypeVariable()
                    .orElseThrow(() -> new NoSuchElementException("No value present"));
            if (!messageParameter.getType().isAssignableFrom(messageBodyType)) {
                throw new IllegalArgumentException("Method annotated with @PulsarConsumer taking Message<T> as " +
                        "argument must have T of @PulsarConsumer.messageBodyType");
            }
            useMessageWrapper = true;
        }

        ConsumerBuilder<?> consumer = pulsarClient.newConsumer(schemaResolver.decideSchema(topicAnnotation, messageBodyType));

        topicAnnotation.stringValue("consumerName").ifPresent(consumer::consumerName);

        String[] topics = topicAnnotation.stringValues("topics");
        String topicsPattern = topicAnnotation.stringValue("topicsPattern").orElse(null);

        if (ArrayUtils.isNotEmpty(topics)) {
            consumer.topic(topics);
        } else if (StringUtils.isNotEmpty(topicsPattern)) {
            consumer.topicsPattern(topicsPattern);
            Optional<RegexSubscriptionMode> mode = topicAnnotation.enumValue("subscriptionTopicsMode", RegexSubscriptionMode.class);
            if (mode.isPresent()) {
                consumer.subscriptionTopicsMode(mode.get());
            } else {
                consumer.subscriptionTopicsMode(RegexSubscriptionMode.AllTopics);
            }
            OptionalInt topicsRefresh = topicAnnotation.intValue("patternAutoDiscoveryPeriod");
            if (topicsRefresh.isPresent()) {
                if (topicsRefresh.getAsInt() < 1) {
                    throw new IllegalArgumentException("Topic " + topicsPattern + " refresh time cannot be below 1 second.");
                }
                consumer.patternAutoDiscoveryPeriod(topicsRefresh.getAsInt(), TimeUnit.SECONDS);
            }
        } else {
            throw new IllegalArgumentException("Pulsar consumer requires topics or topicsPattern value");
        }

        if (null != listener) {
            String subscriptionName = listener.stringValue("subscriptionName")
                    .orElse("pulsar-subscription-" + consumerCounter.incrementAndGet());

            consumer.subscriptionName(subscriptionName);

            listener.enumValue("subscriptionType", SubscriptionType.class)
                    .ifPresent(consumer::subscriptionType);

            Optional<String> ackGroupTimeout = listener.stringValue("ackGroupTimeout");
            if (ackGroupTimeout.isPresent()) {
                Duration duration = Duration.parse(ackGroupTimeout.get());
                consumer.acknowledgmentGroupTime(duration.toNanos(), TimeUnit.NANOSECONDS);
            }
        } else {
            consumer.subscriptionName("pulsar-subsription-" + consumerCounter.incrementAndGet());
        }

        topicAnnotation.stringValue("ackTimeout").map(Duration::parse).ifPresent(duration -> {
            long millis = duration.toMillis();
            //TODO: after enforcing Java 11 use Duration#toSeconds
            long seconds = millis / 1000;
            if (1 < seconds) { // pulsar lib demands gt 1 second not gte
                consumer.ackTimeout(millis, TimeUnit.MILLISECONDS);
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
