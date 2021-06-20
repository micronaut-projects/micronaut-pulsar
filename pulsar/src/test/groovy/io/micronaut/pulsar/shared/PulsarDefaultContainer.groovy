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
package io.micronaut.pulsar.shared

import org.apache.pulsar.client.admin.PulsarAdmin
import org.testcontainers.containers.Container
import org.testcontainers.containers.PulsarContainer

class PulsarDefaultContainer implements AutoCloseable {

    static PulsarAdmin PULSAR_ADMIN

    static final PulsarContainer PULSAR_CONTAINER = new PulsarContainer("2.8.0")
    private static final String PULSAR_CLI_ADMIN = "/pulsar/bin/pulsar-admin"

    static void start() {
        if (PULSAR_CONTAINER.isRunning()) return
        PULSAR_CONTAINER.start()
        sleep 1000 // for some reason clusters don't get proper boot this delay helps a bit for awaiting clusters
        PULSAR_CONTAINER
    }

    @Override
    void close() throws Exception {
        PULSAR_ADMIN.close()
        PULSAR_CONTAINER.close()
    }

    static void createNonPartitionedTopic(String topic) {
        start()
        Container.ExecResult result = PULSAR_CONTAINER.execInContainer("/bin/bash", "-c",
                PULSAR_CLI_ADMIN + " topics create $topic")
        if (0 != result.exitCode) {
            String reason = result.stderr ?: result.stdout
            if (!reason.startsWith("This topic already exists"))
                throw new RuntimeException("Unable to create test topic for TLS: $reason")
        }
    }
}
