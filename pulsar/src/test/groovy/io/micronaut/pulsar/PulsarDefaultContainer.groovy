/*
 * Copyright 2021 original authors
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

import org.apache.pulsar.client.admin.PulsarAdmin
import org.testcontainers.containers.PulsarContainer

class PulsarDefaultContainer implements AutoCloseable {

    static PulsarAdmin PULSAR_ADMIN

    static PulsarContainer PULSAR_CONTAINER = new PulsarContainer("2.7.0").with {
        it.start()
        Thread.sleep(1000) // for some reason clusters don't get proper boot this delay helps a bit for awaiting clusters
        PulsarDefaultContainer.PULSAR_ADMIN = PulsarAdmin.builder().serviceHttpUrl(it.httpServiceUrl).build()
        it
    }

    @Override
    void close() throws Exception {
        PulsarDefaultContainer.PULSAR_ADMIN.close()
        PulsarDefaultContainer.PULSAR_CONTAINER.close()
    }

    static void createNonPartitionedTopic(String topic) {
        PulsarDefaultContainer.PULSAR_ADMIN.topics().createNonPartitionedTopic(topic)
    }
}
