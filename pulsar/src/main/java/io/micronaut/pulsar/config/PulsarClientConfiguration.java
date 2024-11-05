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

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ServiceUrlProvider;

import java.util.Optional;
import java.util.Set;

/**
 * Basic requirements for custom and default configuration to create Pulsar client.
 *
 * @author Haris Secic
 * @since 1.0
 */
public interface PulsarClientConfiguration {

    String getServiceUrl();

    /**
     * Optional provider for Pulsar services URL.
     * @return Optional bean for fetching Pulsar services URLs
     */
    default Optional<ServiceUrlProvider> getServiceUrlProvider() {
        return Optional.empty();
    }

    /**
     * @return Authentication method for pulsar clients
     */
    Authentication getAuthentication();

    /**
     * @return Number of threads to allow for Pulsar library.
     */
    default Optional<Integer> getIoThreads() {
        return Optional.empty();
    }

    /**
     * @return Number of threads to allow for listeners from IO threads defined for Pulsar library
     */
    default Optional<Integer> getListenerThreads() {
        return Optional.empty();
    }

    /**
     * @return SSL provider if any for Pulsar to client communication encryption
     */
    Optional<String> getSslProvider();

    Optional<String> getTlsTrustStorePath();

    /**
     * @return trust store password if any, trust stores don't contain any sensitive information
     * but often work better with password in Java context.
     */
    Optional<String> getTlsTrustStorePassword();

    /**
     * @return TLS certificate file path if any for TLS communication between Pulsar and clients.
     */
    Optional<String> getTlsCertFilePath();

    /**
     * Useful in development environment for using with local host or such.
     * @return Whether to verify TLS certificate host or not.
     */
    Optional<Boolean> getTlsVerifyHostname();

    /**
     * @return Allow insecure connection when TLS certificate is set or not.
     */
    Optional<Boolean> getTlsAllowInsecureConnection();

    /**
     * Ciphers like TLS_RSA_WITH_AES_256_GCM_SHA384, TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256.
     * @return Permitted ciphers for TLS.
     */
    Optional<Set<String>> getTlsCiphers();

    /**
     * Protocols like TLSv1.3, TLSv1.2.
     * @return Permitted protocols for TLS.
     */
    Optional<Set<String>> getTlsProtocols();

    /**
     * Useful for avoiding hard coding tenant name into every annotation value for producers,
     * consumers, or readers.
     * @return Default tenant name if any.
     */
    default Optional<String> getDefaultTenant() {
        return Optional.empty();
    }
}
