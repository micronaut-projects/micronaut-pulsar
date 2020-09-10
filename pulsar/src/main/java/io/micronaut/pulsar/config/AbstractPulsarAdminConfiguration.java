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

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.Properties;

public abstract class AbstractPulsarAdminConfiguration<K, V> extends AbstractPulsarConfiguration<K, V> {
    /**
     * Prefix for Pulsar administration configuration.
     */
    public static final String ADMIN_PREFIX = PREFIX + ".admin";
    /**
     * The default REST(ish) API port for Apache Pulsar cluster.
     */
    public static final int DEFAULT_PULSAR_ADMINISTRATION_PORT = 8080;
    /**
     * By default TLS is disabled for Apache Pulsar administration.
     */
    public static final boolean DEFAULT_PULSAR_ADMINISTRATION_USE_TLS = false;
    /**
     * The default Administration API address.
     */
    public static final String DEFAULT_PULSAR_ADMINISTRATION_ENDPOINT = "http://" + DEFAULT_SERVER_HOST_ADDRESS + ":" + DEFAULT_PULSAR_ADMINISTRATION_PORT;

    private String administrationUrl = DEFAULT_PULSAR_ADMINISTRATION_ENDPOINT;

    /**
     * Constructs a new instance.
     *
     * @param config The config to use
     */
    protected AbstractPulsarAdminConfiguration(Properties config) {
        super(config);
    }

    /**
     * @return URL to administration endpoint
     */
    public Optional<String> getAdministrationUrl() {
        return Optional.ofNullable(administrationUrl);
    }

    /**
     * Set URL pointing towards the HTTP service exposed by the pulsar.
     * @param administrationUrl
     */
    public void setAdministrationUrl(@Nullable String administrationUrl) {
        this.administrationUrl = administrationUrl;
    }
}
