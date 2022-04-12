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

import io.micronaut.core.annotation.Internal;
import io.micronaut.messaging.exceptions.MessageListenerException;
import io.micronaut.pulsar.config.PulsarClientConfiguration;
import jakarta.inject.Singleton;

/**
 * Resolve topic name to same value as input if tenant is hardcoded. Otherwise check for ${tenant} and default tenant
 * name specified in the configuration; if present resolve to that value; otherwise throw exception.
 *
 * @author Haris
 * @since 1.2.0
 */
@Internal
@Singleton
final class DefaultTopicResolver implements TopicResolver {

    private final String defaultTenant;

    DefaultTopicResolver(final PulsarClientConfiguration pulsarClientConfiguration) {
        defaultTenant = pulsarClientConfiguration.getDefaultTenant().orElse(null);
    }

    @Override
    public String resolve(final String topic) {
        if (TopicResolver.isDynamicTenantInTopic(topic)) {
            if (null == defaultTenant) {
                throw new MessageListenerException(String.format(
                        "Consumer specified dynamic tenant name in topic %s but no default tenant name set in the configuration",
                        topic));
            }
            return TopicResolver.replaceTenantInTopic(topic, defaultTenant);
        }
        return topic;
    }
}
