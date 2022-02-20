package io.micronaut.pulsar.processor;

import io.micronaut.context.annotation.Requires;
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
@Requires(missingBeans = TopicResolver.class)
final class DefaultTopicResolver implements TopicResolver {

    private final String defaultTenant;

    DefaultTopicResolver(final PulsarClientConfiguration pulsarClientConfiguration) {
        if (pulsarClientConfiguration.getDefaultTenant().isPresent()) {
            defaultTenant = pulsarClientConfiguration.getDefaultTenant().get();
        } else {
            defaultTenant = null;
        }
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
