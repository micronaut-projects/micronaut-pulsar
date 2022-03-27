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
package io.micronaut.pulsar.processor;

import io.micronaut.core.util.StringUtils;
import io.micronaut.multitenancy.exceptions.TenantNotFoundException;
import io.micronaut.pulsar.config.AbstractPulsarConfiguration;

import java.io.Serializable;

/**
 * Resolve tenant ID class representation to Apache Pulsar tenant name (as {@link String}).
 * <p>
 * Tenant identifiers in Micronaut are represented as {@link Serializable} but Apache Pulsar topic needs to know the
 * ID of the tenant in text form thus requirement for a {@link String}.
 *
 * @author Haris
 * @since 1.2.0
 */
public interface TenantNameResolver {

    /**
     * Resolve tenant name as {@link String} from {@link Serializable}
     *
     * @param tenantId Tenant ID resolved from Micronaut {@link io.micronaut.multitenancy.tenantresolver.TenantResolver}
     * @return String representation of tenant name for Apache Pulsar.
     */
    String resolveTenantNameFromId(final Serializable tenantId);

    /**
     * Enforce usage of tenant name
     *
     * @param tenantName tenant name to enforce on next calls
     */
    void overrideTenantName(final String tenantName);

    /**
     * Clear out enforced tenant name set through {@link #overrideTenantName(String)}
     */
    void clearTenantName();

    /**
     * Resolve current tenant name from tenant ID or overridden value set through {@link #overrideTenantName(String)}.
     *
     * @return name resolved from current tenant ID if name is not overridden; otherwise value set {@link #overrideTenantName(String)}.
     * @throws TenantNotFoundException when override value is missing and no request context is available
     */
    String getCurrentTenantName() throws TenantNotFoundException;

    /**
     * Check whether tenant name exists in the current request context or is enforced.
     *
     * @return true if tenant name was set explicitly or available in {@link io.micronaut.multitenancy.tenantresolver.TenantResolver} otherwise false
     */
    boolean hasTenantName();

    /**
     * @return Whether tenant name will be resolved only once throughout application lifetime
     */
    boolean isStaticTenantResolver();

    static boolean isValidTenantName(final String tenant) {
        if (null == tenant || StringUtils.isEmpty(tenant)) {
            return false;
        }
        return tenant.matches(AbstractPulsarConfiguration.TENANT_NAME_VALIDATOR);
    }
}
