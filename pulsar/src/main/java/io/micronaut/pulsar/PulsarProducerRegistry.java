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

import io.micronaut.core.annotation.NonNull;
import org.apache.pulsar.client.api.Producer;

import java.util.Map;
import java.util.Set;

/**
 * A registry of managed {@link Producer} instances key by id and type.
 *
 * @author Haris Secic
 * @since 1.0
 */
public interface PulsarProducerRegistry {

    /**
     * Get all managed producers.
     * @return List of managed producers.
     */
    Map<String, Producer<?>> getProducers();

    /**
     * Get single managed producer by it's name.
     * @param id
     * @return Pulsar producer by given name
     */
    Producer<?> getProducer(@NonNull String id);

    /**
     * Get all managed producer identifiers.
     * @return List of producer names representing their identifiers in registry.
     */
    Set<String> getProducerIds();
}
