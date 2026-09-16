/*
 * Copyright 2017-2026 original authors
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
package io.micronaut.pulsar.shared;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.ApplicationContextConfigurer;
import io.micronaut.context.annotation.ContextConfigurer;
import io.micronaut.context.env.Environment;
import io.micronaut.context.env.PropertySource;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

/**
 * Starts a plain (non-TLS) Pulsar test container and supplies its {@code pulsar.service-url} to the
 * documentation example tests run with the {@code pulsar} environment
 * ({@code @MicronautTest(environments = "pulsar")}).
 * <p>
 * The configurer is written in Java because Micronaut Test calls {@code TestPropertyProvider} before
 * the application context, and with it the GraalPy runtime of the Python examples, exists, so a Python
 * test class cannot supply the container properties. It uses the {@link #configure(ApplicationContext)}
 * callback because the {@link io.micronaut.context.ApplicationContextBuilder} is configured before
 * {@code @MicronautTest} selects the environments, so the {@code pulsar} environment can only be
 * checked on the built context.
 */
@ContextConfigurer
public class PulsarTestConfigurer implements ApplicationContextConfigurer {

    public static final String PULSAR_ENVIRONMENT = "pulsar";
    public static final String PULSAR_VERSION = "3.3.9";

    private static PulsarContainer pulsarContainer;

    @Override
    public void configure(ApplicationContext applicationContext) {
        Environment environment = applicationContext.getEnvironment();
        if (environment.getActiveNames().contains(PULSAR_ENVIRONMENT)) {
            environment.addPropertySource(PropertySource.of(PULSAR_ENVIRONMENT, Map.of(
                "pulsar.service-url", getPulsarBrokerUrl(),
                "pulsar.shutdown-on-subscriber-error", true
            )));
        }
    }

    /**
     * @return the broker URL of the shared Pulsar test container, started on first use
     */
    public static synchronized String getPulsarBrokerUrl() {
        if (pulsarContainer == null) {
            pulsarContainer = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:" + PULSAR_VERSION));
            pulsarContainer.start();
        }
        return pulsarContainer.getPulsarBrokerUrl();
    }
}
