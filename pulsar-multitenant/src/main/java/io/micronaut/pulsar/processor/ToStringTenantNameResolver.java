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
package io.micronaut.pulsar.processor;

import io.micronaut.context.annotation.Requires;
import io.micronaut.core.annotation.Internal;
import io.micronaut.messaging.exceptions.MessagingException;
import io.micronaut.multitenancy.exceptions.TenantNotFoundException;
import io.micronaut.multitenancy.tenantresolver.FixedTenantResolver;
import io.micronaut.multitenancy.tenantresolver.SystemPropertyTenantResolver;
import io.micronaut.multitenancy.tenantresolver.TenantResolver;
import jakarta.inject.Singleton;

import java.io.Serializable;

/**
 * Resolve tenant name from {@link java.io.Serializable} object with {@code toString()} or if object is already a
 * {@link String}, simply cast.
 *
 * @author Haris
 * @since 1.2.0
 */
@Internal
@Singleton
@Requires(missingBeans = TenantNameResolver.class)
final class ToStringTenantNameResolver implements TenantNameResolver {

    private static final ThreadLocal<String> CURRENT_TENANT = new ThreadLocal<>();

    private final TenantResolver tenantResolver;

    public ToStringTenantNameResolver(final TenantResolver tenantResolver) {
        this.tenantResolver = tenantResolver;
    }

    @Override
    public String resolveTenantNameFromId(final Serializable tenantId) {
        final String tenantName;
        if (tenantId instanceof String stringId) {
            tenantName = stringId;
        } else {
            tenantName = tenantId.toString();
        }
        return tenantName;
    }

    @Override
    public void overrideTenantName(final String tenantName) {
        if (!TenantNameResolver.isValidTenantName(tenantName)) {
            throw new MessagingException(String.format("Invalid tenant name %s", tenantName));
        }
        CURRENT_TENANT.set(tenantName);
    }

    @Override
    public void clearTenantName() {
        CURRENT_TENANT.remove();
    }

    @Override
    public String getCurrentTenantName() throws TenantNotFoundException {
        final var currentTenantName = CURRENT_TENANT.get();
        if (null == currentTenantName) {
            return resolveTenantNameFromId(tenantResolver.resolveTenantIdentifier());
        }
        return currentTenantName;
    }

    @Override
    public boolean hasTenantName() {
        final String currentTenantName = CURRENT_TENANT.get();
        if (null == currentTenantName) {
            try {
                tenantResolver.resolveTenantIdentifier();
                return true;
            } catch (final TenantNotFoundException ignore) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isStaticTenantResolver() {
        return tenantResolver instanceof FixedTenantResolver || tenantResolver instanceof SystemPropertyTenantResolver;
    }
}
