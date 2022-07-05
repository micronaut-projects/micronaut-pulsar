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

import io.micronaut.pulsar.shared.conf.ClientConf
import io.micronaut.pulsar.shared.conf.StandaloneConf
import org.assertj.core.util.Files
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.Container
import org.testcontainers.containers.PulsarContainer
import org.testcontainers.utility.DockerImageName

abstract class PulsarTls {

    public static final int HTTPS = 8443
    public static final int BROKER_SSL = 6651
    private static final String caConfPath = "/my-ca"
    private static final PulsarContainer PULSAR_CONTAINER =
            new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:2.10.0"))
    private static ClassLoader resourceLoader
    private static final String PULSAR_CLI_ADMIN = "/pulsar/bin/pulsar-admin"

    static {
        resourceLoader = ClassLoader.getSystemClassLoader()

        String standalone = createStandaloneConfFile().path
        String client = createClientConf().path

        String brokerCert = resourceLoader.getResource("broker.cert.pem").path
        PULSAR_CONTAINER.addFileSystemBind(new File(brokerCert).path, "$caConfPath/broker.cert.pem", BindMode.READ_ONLY)
        String brokerKey = resourceLoader.getResource("broker.key-pk8.pem").path
        PULSAR_CONTAINER.addFileSystemBind(new File(brokerKey).path, "$caConfPath/broker.key-pk8.pem", BindMode.READ_ONLY)
        String caCert = resourceLoader.getResource("ca.cert.pem").path
        PULSAR_CONTAINER.addFileSystemBind(new File(caCert).path, "$caConfPath/ca.cert.pem", BindMode.READ_ONLY)

        PULSAR_CONTAINER.addFileSystemBind(standalone, "/pulsar/conf/standalone.conf", BindMode.READ_ONLY)
        PULSAR_CONTAINER.addFileSystemBind(client, "/pulsar/conf/client.conf", BindMode.READ_ONLY)

        PULSAR_CONTAINER.addExposedPorts(HTTPS, BROKER_SSL)
        PULSAR_CONTAINER.start()
        createTlsTopic()
    }

    static String getPulsarBrokerTlsUrl() {
        return String.format("pulsar+ssl://%s:%s", PULSAR_CONTAINER.host, PULSAR_CONTAINER.getMappedPort(BROKER_SSL))
    }

    static String getPulsarBrokerUrl() {
        return PULSAR_CONTAINER.pulsarBrokerUrl
    }

    private static File createStandaloneConfFile() {
        String text = StandaloneConf.getContent()
        text = text.replace("tlsCertificateFilePath=", "tlsCertificateFilePath=$caConfPath/broker.cert.pem")
        text = text.replace("tlsKeyFilePath=", "tlsKeyFilePath=$caConfPath/broker.key-pk8.pem")
        text = text.replace("tlsTrustCertsFilePath=", "tlsTrustCertsFilePath=$caConfPath/ca.cert.pem")
        File tmp = Files.newTemporaryFile()
        tmp.write(text)
        return tmp
    }

    private static File createClientConf() {
        String text = ClientConf.getContent()
        text = text.replace("tlsTrustCertsFilePath=", "tlsTrustCertsFilePath=$caConfPath/ca.cert.pem")
        File tmp = Files.newTemporaryFile()
        tmp.write(text)
        return tmp
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
