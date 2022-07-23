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
import io.micronaut.pulsar.dynamic.ConsumerDynamicTenantTopicTester
import io.micronaut.pulsar.dynamic.FakeClient
import io.micronaut.pulsar.dynamic.MessageResponse
import io.micronaut.pulsar.shared.PulsarTls
import io.micronaut.runtime.server.EmbeddedServer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise
import spock.util.concurrent.BlockingVariables

import java.time.Duration

@Stepwise
class DynamicTenantTopicSpec extends Specification {

    public static final String PULSAR_DYNAMIC_TENANT_TEST_TOPIC = 'persistent://${tenant}/default/other2'
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
        dynamicConsumerTester.blockers = vars
        String message = 'This should be received'
        FakeClient fakeClient = context.getBean(FakeClient)

        when:
        fakeClient.addTenantConsumer(DynamicTenantTopicSpec.TENANT_1).block(Duration.ofMinutes(1))
        fakeClient.addTenantConsumer(DynamicTenantTopicSpec.TENANT_2).block(Duration.ofMinutes(1))
        String messageId1 = fakeClient.sendMessage(DynamicTenantTopicSpec.TENANT_1, message)
                .block(Duration.ofMinutes(1))
        MessageResponse readerMessage1 = fakeClient.getNextMessage(DynamicTenantTopicSpec.TENANT_1)
                .block(Duration.ofMinutes(1))
        String messageId2 = fakeClient.sendMessage(DynamicTenantTopicSpec.TENANT_2, message)
                .block(Duration.ofMinutes(1))
        MessageResponse readerMessage2 = fakeClient.getNextMessage(DynamicTenantTopicSpec.TENANT_2)
                .block(Duration.ofMinutes(1))

        then:
        null != messageId1
        messageId1 == readerMessage1.messageId
        message == readerMessage1.message
        (vars.getProperty(messageId1.toString()) as String).contains(message)
        (vars.getProperty(messageId1.toString()) as String).endsWith(DynamicTenantTopicSpec.TENANT_1)
        messageId2 == readerMessage2.messageId
        message == readerMessage2.message
        (vars.getProperty(messageId2) as String).contains(message)
        (vars.getProperty(messageId2) as String).endsWith(DynamicTenantTopicSpec.TENANT_2)
    }

}
