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
package io.micronaut.pulsar.events;

import io.micronaut.core.annotation.NonNull;
import java.io.Serializable;

/**
 * Simple event to publish on new tenant discovery. Event is used by
 * {@link io.micronaut.pulsar.processor.PulsarMultiTenantConsumerProcessor} in case tenants were nat available at the
 * time of initialization of consumer bean(s). It's possible to pre-define list of tenant in some way and
 *
 * @author Haris
 * @since 1.2.0
 */
public final class PulsarTenantDiscoveredEvent {
    private final Serializable tenant;

    public PulsarTenantDiscoveredEvent(@NonNull final Serializable tenant) {
        this.tenant = tenant;
    }

    public Serializable getTenant() {
        return tenant;
    }
}
