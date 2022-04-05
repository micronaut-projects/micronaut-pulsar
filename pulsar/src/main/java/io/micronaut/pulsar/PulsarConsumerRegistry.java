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
package io.micronaut.pulsar;

import io.micronaut.core.annotation.NonNull;
import org.apache.pulsar.client.api.Consumer;

import java.util.Map;
import java.util.Set;

/**
 * A registry for created Pulsar consumers.
 *
 * @author Haris Secic
 * @since 1.0
 */
public interface PulsarConsumerRegistry {

    Map<String, Consumer<?>> getConsumers();

    <T> Consumer<T> getConsumer(@NonNull String id);

    Set<String> getConsumerIds();

    boolean isPaused(@NonNull String id);

    void pause(@NonNull String id);

    void resume(@NonNull String id);
}
