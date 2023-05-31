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
package io.micronaut.pulsar.shared


import org.testcontainers.containers.BindMode
import org.testcontainers.containers.Container
import org.testcontainers.containers.PulsarContainer
import org.testcontainers.containers.output.OutputFrame
import org.testcontainers.utility.DockerImageName

abstract class PulsarTls {

    public static final String PULSAR_VERSION = "3.0.0"

    public static final int HTTPS = 8443
    public static final int BROKER_SSL = 6651
    private static final PulsarContainer PULSAR_CONTAINER =
            new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:${PULSAR_VERSION}"))
    private static ClassLoader resourceLoader
    private static final String PULSAR_CLI_ADMIN = "/pulsar/bin/pulsar-admin"

    static {
        resourceLoader = ClassLoader.getSystemClassLoader()

        final var brokerCert = resourceLoader.getResource("broker.cert.pem").path
        PULSAR_CONTAINER.addFileSystemBind(new File(brokerCert).path, "/my-ca/broker.cert.pem", BindMode.READ_ONLY)
        final var brokerKey = resourceLoader.getResource("broker.key-pk8.pem").path
        PULSAR_CONTAINER.addFileSystemBind(new File(brokerKey).path, "/my-ca/broker.key-pk8.pem", BindMode.READ_ONLY)
        final var caCert = resourceLoader.getResource("ca.cert.pem").path
        PULSAR_CONTAINER.addFileSystemBind(new File(caCert).path, "/my-ca/ca.cert.pem", BindMode.READ_ONLY)

        final var standaloneConfFile = new File(resourceLoader.getResource("standalone.conf").path)
        standaloneConfFile.setWritable(true,false)
        standaloneConfFile.setReadable(true,false)
        standaloneConfFile.setExecutable(true,false)
        final var standalone = standaloneConfFile.path
        final var clientConfFile = new File(resourceLoader.getResource("client.conf").path)
        clientConfFile.setWritable(true, false)
        clientConfFile.setReadable(true, false)
        clientConfFile.setExecutable(true, false)
        final var client = clientConfFile.path

        PULSAR_CONTAINER.addFileSystemBind(standalone, "/pulsar/conf/standalone.conf", BindMode.READ_WRITE)
        PULSAR_CONTAINER.addFileSystemBind(client, "/pulsar/conf/client.conf", BindMode.READ_WRITE)

        PULSAR_CONTAINER.addExposedPorts(HTTPS, BROKER_SSL)
        try {
            PULSAR_CONTAINER.start()
        } catch (Exception e) {
            throw new Exception(PULSAR_CONTAINER.getLogs(OutputFrame.OutputType.STDERR), e)
        }
        createTlsTopic()
    }

    static String getPulsarBrokerTlsUrl() {
        return String.format("pulsar+ssl://%s:%s", PULSAR_CONTAINER.host, PULSAR_CONTAINER.getMappedPort(BROKER_SSL))
    }

    static String getPulsarBrokerUrl() {
        return PULSAR_CONTAINER.pulsarBrokerUrl
    }

    private static void createTlsTopic() {
        PULSAR_CONTAINER.execInContainer("/bin/bash", "-c", PULSAR_CLI_ADMIN + " namespaces set-is-allow-auto-update-schema --enable public/default")
        Container.ExecResult result = PULSAR_CONTAINER.execInContainer("/bin/bash", "-c", PULSAR_CLI_ADMIN + " topics create persistent://public/default/test-tls")
        if (0 != result.exitCode) throw new RuntimeException("Unable to create test topic for TLS")
        Container.ExecResult list = PULSAR_CONTAINER.execInContainer("/bin/bash", "-c", PULSAR_CLI_ADMIN + " topics list public/default")
        int retries = 10
        while (!list.stdout.contains("persistent://public/default/test-tls")) {
            --retries
            if (0 == retries) throw new RuntimeException("Could not get pulsar topics to create")
        }
    }

    static createTopic(final String topic) {
        Container.ExecResult result = PULSAR_CONTAINER.execInContainer('/bin/bash', '-c', PULSAR_CLI_ADMIN + " topics create $topic")
        if (0 != result.exitCode) {
            String reason = result.stderr ?: result.stdout
            if (!reason.startsWith("This topic already exists"))
                throw new RuntimeException("Unable to create test topic for TLS: $reason")
        }
    }

    static createTenant(final String tenant) {
        Container.ExecResult result = PULSAR_CONTAINER.execInContainer('/bin/bash', '-c', PULSAR_CLI_ADMIN + " tenants create $tenant")
        if (0 != result.exitCode) {
            String reason = result.stderr ?: result.stdout
            if (!reason.startsWith("This topic already exists"))
                throw new RuntimeException("Unable to create test topic for TLS: $reason")
        }
        result = PULSAR_CONTAINER.execInContainer('/bin/bash', '-c', PULSAR_CLI_ADMIN + " namespaces create $tenant/default")
        if (0 != result.exitCode) {
            String reason = result.stderr ?: result.stdout
            if (!reason.startsWith("This topic already exists"))
                throw new RuntimeException("Unable to create test tenant for TLS: $reason")
        }
        result = PULSAR_CONTAINER.execInContainer('/bin/bash', '-c', PULSAR_CLI_ADMIN + " namespaces set-retention -s 1T -t -1 $tenant/default")
        if (0 != result.exitCode) {
            String reason = result.stderr ?: result.stdout
            if (!reason.startsWith("Unable to set namespace retention"))
                throw new RuntimeException("Unable to create test tenant for TLS: $reason")
        }
    }
}
