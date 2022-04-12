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
package io.micronaut.pulsar

import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.Environment
import io.micronaut.multitenancy.tenantresolver.TenantResolver
import io.micronaut.pulsar.processor.TenantNameResolver
import io.micronaut.pulsar.processor.TopicResolver
import io.micronaut.pulsar.shared.PulsarTls
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise

@Stepwise
class PulsarConfigurationSpec extends Specification {

    @Shared
    @AutoCleanup
    ApplicationContext context

    public static final TENANT_NAME = 'public'

    void setupSpec() {
        context = ApplicationContext.run(
                ['pulsar.service-url'                                   : PulsarTls.pulsarBrokerUrl,
                 'pulsar.shutdown-on-subscriber-error'                  : true,
                 'spec.name'                                            : getClass().simpleName,
                 'micronaut.multitenancy.tenantresolver.fixed.enabled'  : true,
                 'micronaut.multitenancy.tenantresolver.fixed.tenant-id': PulsarConfigurationSpec.TENANT_NAME],
                Environment.TEST
        )
    }

    void "test load configuration"() {
        expect:
        context.isRunning()
        context.containsBean(TenantResolver)
        context.containsBean(TopicResolver)
        context.containsBean(TenantNameResolver)
        context.getBean(TenantNameResolver).getCurrentTenantName() == PulsarConfigurationSpec.TENANT_NAME
    }
}
