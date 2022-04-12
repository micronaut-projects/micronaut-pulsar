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

import java.util.Optional;

/**
 * Raise error event on failed subscription. Gives more flexibility than throwing exceptions. Developers decide how to
 * handle problems with Pulsar connections.
 *
 * @author Haris Secic
 * @since 1.0
 */
public final class ConsumerSubscriptionFailedEvent implements PulsarFailureEvent {

    private final Throwable sourceError;
    private final String consumerName;
    private final String reason;

    public ConsumerSubscriptionFailedEvent(Throwable sourceError, String consumerName) {
        this.sourceError = sourceError;
        this.consumerName = consumerName;
        String error = "";
        if (null != sourceError) {
            error = sourceError.getMessage();
        }
        this.reason = String.format("Consumer %s failed to subscribe. %s", consumerName, error);
    }

    public Optional<Throwable> getSourceError() {
        return Optional.ofNullable(sourceError);
    }

    public String getConsumerName() {
        return consumerName;
    }

    @Override
    public String getReason() {
        return reason;
    }
}
