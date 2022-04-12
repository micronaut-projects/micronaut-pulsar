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
package io.micronaut.pulsar.dynamic

import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.ApplicationEventPublisher
import io.micronaut.http.HttpRequest
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.annotation.Filter
import io.micronaut.http.filter.HttpServerFilter
import io.micronaut.http.filter.ServerFilterChain
import io.micronaut.multitenancy.tenantresolver.TenantResolver
import io.micronaut.pulsar.events.PulsarTenantDiscoveredEvent
import org.reactivestreams.Publisher

@Requires(property = 'spec.name', value = 'DynamicTenantTopicSpec')
@Filter(Filter.MATCH_ALL_PATTERN)
class ResolveTenantsFilter implements HttpServerFilter {

    final TenantResolver tenantResolver
    final ApplicationEventPublisher<PulsarTenantDiscoveredEvent> tenantPublisher

    ResolveTenantsFilter(final TenantResolver tenantResolver,
                         final ApplicationEventPublisher<PulsarTenantDiscoveredEvent> tenantPublisher) {
        this.tenantResolver = tenantResolver
        this.tenantPublisher = tenantPublisher
    }

    @Override
    Publisher<MutableHttpResponse<?>> doFilter(HttpRequest<?> request, ServerFilterChain chain) {
        final Serializable tenant = tenantResolver.resolveTenantIdentifier()
        //block and await instantiation of the consumers since it will pass out on first (and only in this case)
        //message from the producer
        tenantPublisher.publishEvent(new PulsarTenantDiscoveredEvent(tenant));
        return chain.proceed(request)
    }
}
