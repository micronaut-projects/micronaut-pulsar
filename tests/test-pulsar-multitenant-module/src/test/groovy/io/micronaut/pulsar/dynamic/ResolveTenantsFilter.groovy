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
package io.micronaut.pulsar.dynamic

import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.ApplicationEventPublisher
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.Filter
import io.micronaut.http.filter.FilterChain
import io.micronaut.http.filter.HttpFilter
import io.micronaut.multitenancy.tenantresolver.TenantResolver
import io.micronaut.pulsar.events.PulsarTenantDiscoveredEvent
import org.reactivestreams.Publisher

@Requires(property = 'spec.name', value = 'DynamicTenantTopicSpec')
@Filter("/**")
class ResolveTenantsFilter implements HttpFilter {

    final TenantResolver tenantResolver
    final ApplicationEventPublisher<PulsarTenantDiscoveredEvent> tenantPublisher

    ResolveTenantsFilter(final TenantResolver tenantResolver,
                         final ApplicationEventPublisher<PulsarTenantDiscoveredEvent> tenantPublisher) {
        this.tenantResolver = tenantResolver
        this.tenantPublisher = tenantPublisher
    }

    @Override
    Publisher<? extends HttpResponse<?>> doFilter(HttpRequest<?> request, FilterChain chain) {
        final Serializable tenant = tenantResolver.resolveTenantIdentifier()
        tenantPublisher.publishEvent(new PulsarTenantDiscoveredEvent(tenant))
        return chain.proceed(request)
    }
}
