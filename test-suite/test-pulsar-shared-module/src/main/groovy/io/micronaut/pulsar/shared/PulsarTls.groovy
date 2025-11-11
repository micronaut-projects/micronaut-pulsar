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


import org.testcontainers.containers.Container
import org.testcontainers.containers.PulsarContainer
import org.testcontainers.containers.output.OutputFrame
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile

abstract class PulsarTls {

    public static final String PULSAR_VERSION = "3.3.9"

    public static final int HTTPS = 8443
    public static final int BROKER_SSL = 6651
    private static final PulsarContainer PULSAR_CONTAINER =
            new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:${PULSAR_VERSION}"))
    private static final String PULSAR_CLI_ADMIN = "/pulsar/bin/pulsar-admin"

    static {
        PULSAR_CONTAINER
                .withCopyFileToContainer(MountableFile.forClasspathResource("ca.cert.pem", 0777), "/pulsar/certs/ca.cert.pem")
                .withCopyFileToContainer(MountableFile.forClasspathResource("broker.cert.pem", 0777), "/pulsar/certs/broker.cert.pem")
                .withCopyFileToContainer(MountableFile.forClasspathResource("broker.key-pk8.pem", 0777), "/pulsar/certs/broker.key-pk8.pem")

        PULSAR_CONTAINER.addExposedPorts(HTTPS, BROKER_SSL)
        try {
            PULSAR_CONTAINER
                    .withEnv("PULSAR_PREFIX_brokerServicePortTls", "6651")
                    .withEnv("PULSAR_PREFIX_webServicePortTls", "8443")
                    .withEnv("PULSAR_PREFIX_tlsTrustCertsFilePath", "/pulsar/certs/ca.cert.pem")
                    .withEnv("PULSAR_PREFIX_tlsKeyFilePath", "/pulsar/certs/broker.key-pk8.pem")
                    .withEnv("PULSAR_PREFIX_tlsCertificateFilePath", "/pulsar/certs/broker.cert.pem")
                    .withEnv("PULSAR_PREFIX_tlsRequireTrustedClientCertOnConnect", "false")
                    .withEnv("PULSAR_PREFIX_tlsEnableHostnameVerification", "false")
                    .withEnv("PULSAR_PREFIX_tlsTrustStore", "NONE")
                    .start()
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
