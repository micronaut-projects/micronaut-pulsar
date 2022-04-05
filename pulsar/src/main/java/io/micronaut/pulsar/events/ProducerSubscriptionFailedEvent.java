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

/**
 * Raise error event on failed producer subscription.
 *
 * @author Haris Secic
 * @since 1.0
 */
public final class ProducerSubscriptionFailedEvent implements PulsarFailureEvent {

    private final String producerName;
    private final String reason;
    private final Throwable sourceError;

    public ProducerSubscriptionFailedEvent(String producerName, Throwable sourceError) {
        this.producerName = producerName;
        this.sourceError = sourceError;
        String error = "";
        if (null != sourceError) {
            error = sourceError.getMessage();
        }
        this.reason = String.format("Producer %s failed to connect. %s", producerName, error);
    }

    @Override
    public String getReason() {
        return reason;
    }

    public Throwable getSourceError() {
        return sourceError;
    }

    public String getProducerName() {
        return producerName;
    }
}
