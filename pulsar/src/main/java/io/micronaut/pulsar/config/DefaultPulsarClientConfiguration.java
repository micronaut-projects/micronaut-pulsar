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
package io.micronaut.pulsar.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.context.exceptions.ConfigurationException;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.util.StringUtils;
import io.micronaut.pulsar.annotation.PulsarServiceUrlProvider;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;

import java.net.URL;
import java.util.*;

import static io.micronaut.core.naming.conventions.StringConvention.RAW;
import static io.micronaut.pulsar.config.AbstractPulsarConfiguration.PREFIX;

/**
 * Default properties holder for Pulsar client configuration.
 *
 * @author Haris Secic
 * @since 1.0
 */
@ConfigurationProperties(PREFIX)
@Requires(PREFIX)
@Requires(missingBeans = PulsarClientConfiguration.class)
public final class DefaultPulsarClientConfiguration extends AbstractPulsarConfiguration implements PulsarClientConfiguration {

    private Integer ioThreads;
    private Integer listenerThreads;
    private String sslProvider;
    private String serviceUrl;
    private final Optional<ServiceUrlProvider> serviceUrlProvider;
    private Authentication pulsarAuthentication;
    private URL oauthIssuerUrl;
    private URL oauthCredentialsUrl;
    private String oauthAudience;
    private Boolean useDeadLetterQueue;
    private int defaultMaxRetryDlq = 3;
    private String tlsTrustStorePath;
    private String tlsCertFilePath;
    private Boolean tlsVerifyHostname;
    private Boolean tlsAllowInsecureConnection;
    private Set<String> tlsCiphers;
    private Set<String> tlsProtocols;
    private Boolean shutdownOnSubscriberError;
    private String defaultTenant;

    /**
     * Constructs the default Pulsar Client configuration.
     *
     * @param environment        Environment
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
     * @param listenerThreads Number of threads to use with message example.java.listeners.
     */
    public void setListenerThreads(Integer listenerThreads) {
        this.listenerThreads = listenerThreads;
    }

    public void setAuthenticationJwt(@Nullable String authenticationJwt) {
        pulsarAuthentication = new AuthenticationToken(authenticationJwt);
    }

    public Optional<String> getSslProvider() {
        return Optional.ofNullable(sslProvider);
    }

    public void setTlsTrustStorePath(String tlsTrustStorePath) {
        this.tlsTrustStorePath = tlsTrustStorePath;
    }

    @Override
    public Optional<String> getTlsTrustStorePath() {
        return Optional.ofNullable(tlsTrustStorePath);
    }

    public void setTlsCertFilePath(String tlsCertFilePath) {
        this.tlsCertFilePath = tlsCertFilePath;
    }

    @Override
    public Optional<String> getTlsCertFilePath() {
        return Optional.ofNullable(tlsCertFilePath);
    }

    public void setTlsVerifyHostname(Boolean tlsVerifyHostname) {
        this.tlsVerifyHostname = tlsVerifyHostname;
    }

    @Override
    public Optional<Boolean> getTlsVerifyHostname() {
        return Optional.ofNullable(tlsVerifyHostname);
    }

    public void setTlsAllowInsecureConnection(Boolean tlsAllowInsecureConnection) {
        this.tlsAllowInsecureConnection = tlsAllowInsecureConnection;
    }

    @Override
    public Optional<Boolean> getTlsAllowInsecureConnection() {
        return Optional.ofNullable(tlsAllowInsecureConnection);
    }

    public void setTlsCiphers(Set<String> tlsCiphers) {
        this.tlsCiphers = tlsCiphers;
    }

    @Override
    public Optional<Set<String>> getTlsCiphers() {
        return Optional.ofNullable(tlsCiphers);
    }

    public void setTlsProtocols(Set<String> tlsProtocols) {
        this.tlsProtocols = tlsProtocols;
    }

    @Override
    public Optional<Set<String>> getTlsProtocols() {
        return Optional.ofNullable(tlsProtocols);
    }

    /**
     * Defaults to default JVM provider.
     *
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
        return serviceUrlProvider;
    }

    @Override
    public Authentication getAuthentication() {
        return Optional.ofNullable(pulsarAuthentication).orElse(DEFAULT_PULSAR_AUTHENTICATION);
    }

    @Override
    public boolean getShutdownOnSubscriberError() {
        return Optional.ofNullable(shutdownOnSubscriberError).orElse(false);
    }

    /**
     * @param shutdownOnSubscriberError should application shutdown if any of subscriptions fail
     */
    public void setShutdownOnSubscriberError(Boolean shutdownOnSubscriberError) {
        this.shutdownOnSubscriberError = shutdownOnSubscriberError;
    }

    /**
     * Must be set for usage with the OAuth2 authentication.
     *
     * @return String representing client application willing to listen to
     */
    public Optional<String> getOauthAudience() {
        return Optional.ofNullable(oauthAudience);
    }

    /**
     * @param oauthAudience OAuth2 audience
     */
    public void setOauthAudience(String oauthAudience) {
        this.oauthAudience = oauthAudience;
        generateAuthenticationOAuth2();
    }

    public Optional<URL> getOauthCredentialsUrl() {
        return Optional.ofNullable(oauthCredentialsUrl);
    }

    /**
     * @param oauthCredentialsUrl URL or a path to a file containing client id, client secret, and such for OAuth2 client application.
     */
    public void setOauthCredentialsUrl(URL oauthCredentialsUrl) {
        this.oauthCredentialsUrl = oauthCredentialsUrl;
        generateAuthenticationOAuth2();
    }

    public URL getOauthIssuerUrl() {
        return oauthIssuerUrl;
    }

    /**
     * @param oauthIssuerUrl URL of the OAuth2 Token issuer
     */
    public void setOauthIssuerUrl(URL oauthIssuerUrl) {
        this.oauthIssuerUrl = oauthIssuerUrl;
        generateAuthenticationOAuth2();
    }

    public Boolean getUseDeadLetterQueue() {
        return Optional.ofNullable(useDeadLetterQueue).orElse(true);
    }

    /**
     * If not set defaults to true which means that after max number of retries failed message is sent to DLQ and won't
     * be resent again.
     *
     * @param useDeadLetterQueue Use DLQ for Pulsar Consumers by default or not.
     */
    public void setUseDeadLetterQueue(Boolean useDeadLetterQueue) {
        this.useDeadLetterQueue = useDeadLetterQueue;
    }

    public int getDefaultMaxRetryDlq() {
        return defaultMaxRetryDlq;
    }

    /**
     * If not set defaults to 3. {@code #useDeadLetterQueue} must be enabled or else this value is ignored.
     *
     * @param defaultMaxRetryDlq Deafult max number of retries before sending message to DLQ for all consumers.
     */
    public void setDefaultMaxRetryDlq(int defaultMaxRetryDlq) {
        if (defaultMaxRetryDlq < 1) {
            throw new IllegalArgumentException("Default number of max retries for DLQ must be greater than 0");
        }
        this.defaultMaxRetryDlq = defaultMaxRetryDlq;
    }

    private void generateAuthenticationOAuth2() {
        if (null == oauthIssuerUrl || null == oauthCredentialsUrl || StringUtils.isEmpty(oauthAudience)) {
            return;
        }
        pulsarAuthentication = AuthenticationFactoryOAuth2.clientCredentials(oauthIssuerUrl, oauthCredentialsUrl, oauthAudience);
    }

    private static Properties resolveDefaultConfiguration(Environment environment) {
        Map<String, Object> values = environment.containsProperties(PREFIX)
                ? environment.getProperties(PREFIX, RAW)
                : Collections.emptyMap();
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

    @Override
    public Optional<String> getDefaultTenant() {
        return Optional.ofNullable(defaultTenant);
    }

    /**
     * Use in combination with ${tenant} placeholder in {@link io.micronaut.pulsar.annotation.PulsarConsumer#topic()},
     * {@link io.micronaut.pulsar.annotation.PulsarConsumer#topics()},
     * {@link io.micronaut.pulsar.annotation.PulsarConsumer#topicsPattern()},
     * {@link io.micronaut.pulsar.annotation.PulsarReader#topic()},
     * {@link io.micronaut.pulsar.annotation.PulsarProducer#topic()} to avoid hardcoding tenant name into source code
     * through mentioned annotations.
     *
     * This property will be ignored if multi-tenancy module is enabled.
     *
     * @param defaultTenant Default Apache Pulsar tenant name to apply on dynamic topic names.
     */
    public void setDefaultTenant(final String defaultTenant) {
        if (null != defaultTenant && !defaultTenant.matches(TENANT_NAME_VALIDATOR)) {
            throw new ConfigurationException(String.format(
                    "Default tenant value %s is not a valid tenant name for Apache Pulsar",
                    defaultTenant));
        }
        this.defaultTenant = defaultTenant;
    }
}
