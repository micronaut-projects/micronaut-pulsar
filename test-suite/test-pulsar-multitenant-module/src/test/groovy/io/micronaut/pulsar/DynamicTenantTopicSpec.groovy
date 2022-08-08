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
import io.micronaut.pulsar.dynamic.ConsumerDynamicTenantTopicTester
import io.micronaut.pulsar.dynamic.DynamicReader
import io.micronaut.pulsar.dynamic.FakeClient
import io.micronaut.pulsar.processor.TenantNameResolver
import io.micronaut.pulsar.shared.PulsarTls
import io.micronaut.runtime.server.EmbeddedServer
import org.apache.pulsar.client.api.Message
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise
import spock.util.concurrent.BlockingVariables

@Stepwise
class DynamicTenantTopicSpec extends Specification {

    public static final String PULSAR_DYNAMIC_TENANT_TEST_TOPIC = 'persistent://${tenant}/default/dynamicTenantTest'
    public static final String TENANT_1 = 't1'
    public static final String TENANT_2 = 't2'

    @Shared
    @AutoCleanup
    ApplicationContext context

    @Shared
    @AutoCleanup
    EmbeddedServer server


    void setupSpec() {
        PulsarTls.createTenant(DynamicTenantTopicSpec.TENANT_1)
        PulsarTls.createTenant(DynamicTenantTopicSpec.TENANT_2)
        PulsarTls.createTopic(DynamicTenantTopicSpec.PULSAR_DYNAMIC_TENANT_TEST_TOPIC.replace('${tenant}', DynamicTenantTopicSpec.TENANT_1))
        PulsarTls.createTopic(DynamicTenantTopicSpec.PULSAR_DYNAMIC_TENANT_TEST_TOPIC.replace('${tenant}', DynamicTenantTopicSpec.TENANT_2))
        server = ApplicationContext.run(EmbeddedServer,
                ['pulsar.service-url'                                          : PulsarTls.pulsarBrokerUrl,
                 'pulsar.shutdown-on-subscriber-error'                         : true,
                 'spec.name'                                                   : getClass().simpleName,
                 'micronaut.http.client.read-timeout'                          : '5m',
                 'micronaut.multitenancy.tenantresolver.httpheader.enabled'    : true,
                 // Micronaut ignores @Header.name for some reason and always adds -
                 'micronaut.multitenancy.tenantresolver.httpheader.header-name': 'tenant-id'],
                Environment.TEST
        ) as EmbeddedServer
        context = server.applicationContext
    }

    void "test consumer and producer instantiate by each resolved tenant"() {
        given:
        BlockingVariables vars = new BlockingVariables(65)
        ConsumerDynamicTenantTopicTester dynamicConsumerTester = context.getBean(ConsumerDynamicTenantTopicTester.class)
        DynamicReader dynamicReaderTester = context.getBean(DynamicReader.class)
        dynamicConsumerTester.blockers = vars
        String message = 'This should be received'
        FakeClient fakeClient = context.getBean(FakeClient)
        TenantNameResolver tenantNameResolver = context.getBean(TenantNameResolver.class)

        when:
        fakeClient.addTenantConsumer(DynamicTenantTopicSpec.TENANT_1)
        fakeClient.addTenantConsumer(DynamicTenantTopicSpec.TENANT_2)
        String messageId1 = fakeClient.sendMessage(DynamicTenantTopicSpec.TENANT_1, message)
        tenantNameResolver.overrideTenantName(DynamicTenantTopicSpec.TENANT_1)
        Message<String> readerMessage1 = dynamicReaderTester.read()
        tenantNameResolver.clearTenantName()
        String messageId2 = fakeClient.sendMessage(DynamicTenantTopicSpec.TENANT_2, message)
        tenantNameResolver.overrideTenantName(DynamicTenantTopicSpec.TENANT_2)
        Message<String> readerMessage2 = dynamicReaderTester.read()
        tenantNameResolver.clearTenantName()

        then:
        null != messageId1
        messageId1 == readerMessage1.messageId.toString()
        message == readerMessage1.value
        (vars.getProperty(messageId1.toString()) as String).contains(message)
        (vars.getProperty(messageId1.toString()) as String).endsWith(DynamicTenantTopicSpec.TENANT_1)
        messageId2 == readerMessage2.messageId.toString()
        message == readerMessage2.value
        (vars.getProperty(messageId2) as String).contains(message)
        (vars.getProperty(messageId2) as String).endsWith(DynamicTenantTopicSpec.TENANT_2)
    }

}
