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
package io.micronaut.pulsar;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.pulsar.config.PulsarClientConfiguration;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientBuilderImpl;

import javax.inject.Singleton;

/**
 * Create bean of PulsarClient type which is required by consumers and producers.
 *
 * @author Haris Secic
 * @since 1.0
 */
@Factory
@Requires(beans = {PulsarClientConfiguration.class})
public final class PulsarClientFactory {

    /**
     * Simple factory method for building main PulsarClient that serves as a connection to Pulsar cluster.
     * @param pulsarClientConfiguration Main configuration for building PulsarClient
     * @return Instance of {@link PulsarClient}
     * @throws PulsarClientException in case any of the required options are missing or malformed
     */
    @Singleton
    public PulsarClient pulsarClient(PulsarClientConfiguration pulsarClientConfiguration) throws PulsarClientException {
        ClientBuilder clientBuilder = new ClientBuilderImpl()
                .authentication(pulsarClientConfiguration.getAuthentication());

        if (pulsarClientConfiguration.getServiceUrlProvider().isPresent()) {
            clientBuilder.serviceUrlProvider(pulsarClientConfiguration.getServiceUrlProvider().get());
        } else { //Because service URL defaults to localhost it's required to first check for providers
            clientBuilder.serviceUrl(pulsarClientConfiguration.getServiceUrl());
        }

        pulsarClientConfiguration.getIoThreads().ifPresent(clientBuilder::ioThreads);
        pulsarClientConfiguration.getListenerThreads().ifPresent(clientBuilder::listenerThreads);
        pulsarClientConfiguration.getSslProvider().ifPresent(clientBuilder::sslProvider);

        return clientBuilder.build();
    }
}
