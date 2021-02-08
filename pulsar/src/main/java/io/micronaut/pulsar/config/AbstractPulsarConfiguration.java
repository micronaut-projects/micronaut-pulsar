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
package io.micronaut.pulsar.config;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;

import javax.annotation.Nonnull;
import java.util.Properties;

public abstract class AbstractPulsarConfiguration<K, V> {
    /**
     * The default Apache Pulsar messaging port.
     */
    public static final int DEFAULT_PULSAR_MESSAGING_PORT = 6650;
    /**
     * The default prefix used for Pulsar configuration.
     */
    public static final String PREFIX = "pulsar";
    /**
     * The default server hostname or IP address.
     */
    public static final String DEFAULT_SERVER_HOST_ADDRESS = "localhost";
    /**
     * The default bootstrap server address for messaging.
     */
    public static final String DEFAULT_BOOTSTRAP_SERVER = "pulsar://" + DEFAULT_SERVER_HOST_ADDRESS + ":" + DEFAULT_PULSAR_MESSAGING_PORT;
    /**
     * By default Pulsar doesn't have any authentication.
     */
    public static final Authentication DEFAULT_PULSAR_AUTHENTICATION = new AuthenticationDisabled();
    /**
     * Regex for validating topic name.
     */
    public static final String TOPIC_NAME_VALIDATOR = "((non-)?persistent://)?(\\w+(-?\\w+)?/){2}(\\w+(-?\\w+)?)";
    /**
     * Regex for validating topic pattern.
     */
    public static final String TOPIC_NAME_PATTERN_VALIDATOR = "((non-)?persistent://)?(\\w+(-?\\w+)?/){2}.+";

    private final Properties config;

    protected AbstractPulsarConfiguration(Properties config) {
        this.config = config;
    }

    /**
     * @return The Pulsar configuration
     */
    public @Nonnull
    Properties getConfig() {
        if (config != null) {
            return config;
        }
        return new Properties();
    }
}
