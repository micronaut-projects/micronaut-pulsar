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
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import io.micronaut.pulsar.annotation.PulsarConsumer
import io.micronaut.pulsar.annotation.PulsarProducer
import io.micronaut.pulsar.annotation.PulsarProducerClient
import io.micronaut.pulsar.annotation.PulsarSubscription
import io.micronaut.pulsar.shared.PulsarTls
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise
import spock.util.concurrent.BlockingVariables

@Stepwise
class FixedTenantTopicResolverSpec extends Specification {

    public static final String PULSAR_FIXED_TENANT_TEST_TOPIC = 'persistent://${tenant}/default/fixedTenantTest'

    @Shared
    @AutoCleanup
    ApplicationContext context

    void setupSpec() {
        PulsarTls.createTopic(FixedTenantTopicResolverSpec.PULSAR_FIXED_TENANT_TEST_TOPIC.replace('${tenant}', 'public'))
        context = ApplicationContext.run(
                ['pulsar.service-url'                                   : PulsarTls.pulsarBrokerUrl,
                 'pulsar.shutdown-on-subscriber-error'                  : true,
                 'spec.name'                                            : getClass().simpleName,
                 'micronaut.multitenancy.tenantresolver.fixed.enabled'  : true,
                 'micronaut.multitenancy.tenantresolver.fixed.tenant-id': 'public'],
                Environment.TEST
        )
    }

    void "test consumer dynamic tenant name loading in topic"() {
        given:
        BlockingVariables vars = new BlockingVariables(65)
        ConsumerFixedTenantTopicTester dynamicConsumerTester = context.getBean(ConsumerFixedTenantTopicTester.class)
        dynamicConsumerTester.blockers = vars
        ProducerFixedTenantTopicTester dynamicProducerTester = context.getBean(ProducerFixedTenantTopicTester.class)
        String message = 'This should be received'

        when:
        MessageId messageId = dynamicProducerTester.send(message)

        then:
        null != messageId
        messageId == vars.getProperty("messageId")
        message == vars.getProperty("value")
    }

    @Requires(property = 'spec.name', value = 'FixedTenantTopicResolverSpec')
    @PulsarSubscription(subscriptionName = "subscriber-dynamic")
    static class ConsumerFixedTenantTopicTester {
        BlockingVariables blockers

        @PulsarConsumer(
                topic = FixedTenantTopicResolverSpec.PULSAR_FIXED_TENANT_TEST_TOPIC,
                consumerName = 'dynamic-topic-consumer',
                subscribeAsync = false)
        void topicListener(Message<String> message) {
            if (null == blockers) {
                return
            }
            blockers.setProperty("messageId", message.messageId)
            blockers.setProperty("value", new String(message.value))
        }
    }

    @Requires(property = 'spec.name', value = 'FixedTenantTopicResolverSpec')
    @PulsarProducerClient
    static interface ProducerFixedTenantTopicTester {
        @PulsarProducer(
                topic = FixedTenantTopicResolverSpec.PULSAR_FIXED_TENANT_TEST_TOPIC,
                producerName = 'dynamic-topic-producer')
        MessageId send(String message)
    }
}
