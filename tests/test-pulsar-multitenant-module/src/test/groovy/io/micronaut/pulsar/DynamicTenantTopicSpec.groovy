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
package io.micronaut.pulsar

import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.Environment
import io.micronaut.pulsar.dynamic.ConsumerDynamicTenantTopicTester
import io.micronaut.pulsar.dynamic.FakeClient
import io.micronaut.pulsar.shared.PulsarTls
import io.micronaut.runtime.server.EmbeddedServer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise
import spock.util.concurrent.BlockingVariables

@Stepwise
class DynamicTenantTopicSpec extends Specification {

    public static final String PULSAR_DYNAMIC_TENANT_TEST_TOPIC = 'persistent://${tenant}/default/other2'
    public static final String TENANT_1 = 't1'

    @Shared
    @AutoCleanup
    ApplicationContext context

    @Shared
    @AutoCleanup
    EmbeddedServer server


    void setupSpec() {
        PulsarTls.createTopic(DynamicTenantTopicSpec.PULSAR_DYNAMIC_TENANT_TEST_TOPIC.replace('${tenant}', 'public'))
        PulsarTls.createTenant(DynamicTenantTopicSpec.TENANT_1)
        PulsarTls.createTopic(DynamicTenantTopicSpec.PULSAR_DYNAMIC_TENANT_TEST_TOPIC.replace('${tenant}', DynamicTenantTopicSpec.TENANT_1))
        server = ApplicationContext.run(EmbeddedServer,
                ['pulsar.service-url'                                      : PulsarTls.pulsarBrokerUrl,
                 'pulsar.shutdown-on-subscriber-error'                     : true,
                 'spec.name'                                               : getClass().simpleName,
                 'micronaut.multitenancy.tenantresolver.httpheader.enabled': true],
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
        String messageId1 = fakeClient.sendMessage(message, 'public').block()
        String messageId2 = fakeClient.sendMessage(message, DynamicTenantTopicSpec.TENANT_1).block()

        then:
        null != messageId1
        null != vars.getProperty('messages')
        messageId1 == (vars.getProperty('messages') as List[0]['messageId'])
        message == (vars.getProperty('messages') as List[0]['value'])
        (vars.getProperty('messages') as List[0]['topic'] as String).contains('public')
        messageId2 == (vars.getProperty('messages') as List[1]['messageId'])
        message == (vars.getProperty('messages') as List[1]['value'])
        (vars.getProperty('messages') as List[1]['topic'] as String).contains(DynamicTenantTopicSpec.TENANT_1)
    }


}
