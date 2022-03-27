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

import io.micronaut.context.annotation.Replaces;
import io.micronaut.core.annotation.Internal;
import io.micronaut.messaging.exceptions.MessageListenerException;
import io.micronaut.multitenancy.exceptions.TenantNotFoundException;
import io.micronaut.pulsar.config.AbstractPulsarConfiguration;
import jakarta.inject.Singleton;

/**
 * Topic resolver for multi tenant scenarios. Replaces ${tenant} with the actual Apache Pulsar tenant name.
 *
 * @author Haris
 * @since 1.2.0
 */
@Singleton
@Replaces(bean = TopicResolver.class)
@Internal
final class MultiTenantTopicResolver implements TopicResolver {

    private final static String FORMAT_ID = "%s-%s";
    private final TenantNameResolver tenantNameResolver;

    public MultiTenantTopicResolver(final TenantNameResolver tenantNameResolver) {
        this.tenantNameResolver = tenantNameResolver;
    }

    @Override
    public String resolve(final String topic) throws TenantNotFoundException, MessageListenerException {
        if (!TopicResolver.isDynamicTenantInTopic(topic)) {
            return topic;
        }

        final String tenantName = tenantNameResolver.getCurrentTenantName();

        if (!TenantNameResolver.isValidTenantName(tenantName)) {
            throw new MessageListenerException(String.format(
                "Invalid value for topic: %s while resolving tenant: %s. Tenant name does not match pattern: %s",
                topic,
                tenantName,
                AbstractPulsarConfiguration.TENANT_NAME_VALIDATOR));
        }

        return TopicResolver.replaceTenantInTopic(topic, tenantName);
    }

    @Override
    public String generateIdFromMessagingClientName(final String name, final TopicResolved topicResolved) {
        if (topicResolved.isDynamicTenant()) {
            return String.format(FORMAT_ID, tenantNameResolver.getCurrentTenantName(), name);
        }
        return name;
    }
}
