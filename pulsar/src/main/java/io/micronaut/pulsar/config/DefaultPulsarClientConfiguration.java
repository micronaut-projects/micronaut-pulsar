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
package io.micronaut.pulsar.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.naming.conventions.StringConvention;
import io.micronaut.pulsar.annotation.PulsarServiceUrlProvider;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Default properties holder for Pulsar client configuration.
 *
 * @author Haris Secic
 * @since 1.0
 */
@ConfigurationProperties(AbstractPulsarConfiguration.PREFIX)
@Requires(AbstractPulsarConfiguration.PREFIX)
@Requires(missingBeans = PulsarClientConfiguration.class)
public final class DefaultPulsarClientConfiguration extends AbstractPulsarConfiguration implements PulsarClientConfiguration {

    private Integer ioThreads;
    private Integer listenerThreads;
    private String sslProvider;
    private String serviceUrl;
    private final Optional<ServiceUrlProvider> serviceUrlProvider;
    private Authentication pulsarAuthentication;

    /**
     * Constructs the default Pulsar Client configuration.
     *
     * @param environment Environment
     * @param serviceUrlProvider Pulsars service URL provider
     */
    protected DefaultPulsarClientConfiguration(final Environment environment,
                                               @PulsarServiceUrlProvider final Optional<ServiceUrlProvider> serviceUrlProvider) {
        super(resolveDefaultConfiguration(environment));
        this.serviceUrlProvider = serviceUrlProvider;
    }

    public Optional<Integer> getIoThreads() {
        return Optional.ofNullable(ioThreads);
    }

    /**
     * @param ioThreads Number of threads to use with read operations
     */
    public void setIoThreads(Integer ioThreads) {
        this.ioThreads = ioThreads;
    }

    public Optional<Integer> getListenerThreads() {
        return Optional.ofNullable(listenerThreads);
    }

    /**
     * @param listenerThreads Number of threads to use with message listeners.
     */
    public void setListenerThreads(Integer listenerThreads) {
        this.listenerThreads = listenerThreads;
    }

    public void setAuthenticationJwt(@Nullable String authenticationJwt) {
        this.pulsarAuthentication = new AuthenticationToken(authenticationJwt);
    }

    public Optional<String> getSslProvider() {
        return Optional.ofNullable(sslProvider);
    }

    /**
     * Defaults to default JVM provider.
     * @param sslProvider The name of the security provider used for SSL connections.
     */
    public void setSslProvider(String sslProvider) {
        this.sslProvider = sslProvider;
    }

    /**
     * @return Apache Pulsar cluster address (IP or domain or hostname + port)
     */
    public String getServiceUrl() {
        return Optional.ofNullable(serviceUrl).orElse(DEFAULT_BOOTSTRAP_SERVER);
    }

    /**
     * @param serviceUrl URL to Pulsar cluster
     */
    public void setServiceUrl(@Nullable String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    public Optional<ServiceUrlProvider> getServiceUrlProvider() {
        return this.serviceUrlProvider;
    }

    @Override
    public Authentication getAuthentication() {
        return Optional.ofNullable(this.pulsarAuthentication).orElse(DEFAULT_PULSAR_AUTHENTICATION);
    }

    private static Properties resolveDefaultConfiguration(Environment environment) {
        Map<String, Object> values = environment.containsProperties(PREFIX) ? environment.getProperties(PREFIX, StringConvention.RAW) : Collections.emptyMap();
        Properties properties = new Properties();
        values.forEach((key, value) -> {
            if (ConversionService.SHARED.canConvert(value.getClass(), String.class)) {
                Optional<?> converted = ConversionService.SHARED.convert(value, String.class);
                if (converted.isPresent()) {
                    value = converted.get();
                }
            }
            properties.setProperty(key, value.toString());
        });
        return properties;
    }
}
