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

import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.util.ArrayUtils;
import io.micronaut.messaging.exceptions.MessageListenerException;
import io.micronaut.messaging.exceptions.MessagingException;
import io.micronaut.pulsar.config.AbstractPulsarConfiguration;

import java.util.Arrays;

/**
 * Process input string to determine real topic name for Apache Pulsar. Allows flexibility by avoiding requirement
 * to hardcode full topic names into the code. Useful in scenarios like multi-tenant applications.
 *
 * @author Haris Secic
 * @since 1.2.0
 */
@FunctionalInterface
public interface TopicResolver {
    String resolve(String topic);

    default String generateIdFromMessagingClientName(final String name, final TopicResolved topicResolved) {
        return name;
    }

    static String replaceTenantInTopic(final String topic, final String tenant) {
        return topic.replaceFirst("\\$\\{(tenant)}", tenant);
    }

    static boolean isDynamicTenantInTopic(final String topic) {
        return topic.contains("${tenant}");
    }

    @NonNull
    static TopicResolved extractTopic(final AnnotationValue<?> pulsarAnnotation,
                                      final String forName) {
        //don't remap tenantId value placeholder since it can be empty during initial run
        //verify topic values here since regexp in Pattern annotation causes problems in some scenarios
        final var topic = pulsarAnnotation.stringValue("topic", null)
            .orElse(null);
        // replace util string check with null && isBlank because of SonarCloud
        if (null != topic && !topic.isBlank()) {
            verifyTopicValue(topic, forName);
            return new TopicResolved(topic, false);
        }
        final var topics = pulsarAnnotation.stringValues("topics", null);
        if (ArrayUtils.isNotEmpty(topics)) {
            Arrays.stream(topics).forEach(x -> verifyTopicValue(x, forName));
            return new TopicResolved(topics, false);
        }
        final var topicsPattern = pulsarAnnotation.stringValue("topicsPattern", null)
            .orElse(null);
        // again for SonarCloud
        if (null != topicsPattern && !topicsPattern.isBlank()) {
            if (topicsPattern.matches(AbstractPulsarConfiguration.TOPIC_NAME_PATTERN_VALIDATOR)) {
                return new TopicResolved(topicsPattern, true);
            }
            throw new MessageListenerException(
                "Consumer failed. Invalid topic pattern value %s. Must match %s".formatted(
                    topicsPattern, AbstractPulsarConfiguration.TOPIC_NAME_PATTERN_VALIDATOR
                )
            );
        }
        throw new MessagingException("Missing topic value for %s".formatted(forName));
    }

    private static void verifyTopicValue(final String topic,
                                         final String forName) {
        // null check because of SonarCloud
        if (null != topic && topic.matches(AbstractPulsarConfiguration.TOPIC_NAME_VALIDATOR)) {
            return;
        }
        final var message = "Invalid topic value %s for %s. Must match %s".formatted(
            topic, forName, AbstractPulsarConfiguration.TOPIC_NAME_VALIDATOR
        );
        throw new MessageListenerException(message);
    }

    /**
     * Simple container class for describing resolved topics.
     */
    final class TopicResolved {
        private final Object value;
        private final boolean isPattern;

        TopicResolved(Object value, boolean isPattern) {
            this.value = value;
            this.isPattern = isPattern;
        }

        public String getTopic() {
            if (isArray()) {
                throw new IllegalStateException("Resolving single topic when topic list was used");
            }
            return (String) value;
        }

        public String[] getTopics() {
            if (!isArray()) {
                throw new IllegalStateException("Resolving topic list when single topic was used");
            }
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
    }
}
