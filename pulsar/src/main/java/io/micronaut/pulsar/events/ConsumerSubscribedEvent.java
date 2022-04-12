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
package io.micronaut.pulsar.events;

import org.apache.pulsar.client.api.Consumer;

/**
 * Produced when Pulsar consumer connects to a broker and starts listening to specified topics.
 *
 * @since 1.0
 * @author Haris Secic
 */
public final class ConsumerSubscribedEvent {

    private final Consumer<?> consumer;

    public ConsumerSubscribedEvent(Consumer<?> consumer) {
        this.consumer = consumer;
    }

    public Consumer<?> getConsumer() {
        return consumer;
    }
}
