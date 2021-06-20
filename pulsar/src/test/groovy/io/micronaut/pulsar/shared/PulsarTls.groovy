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


import io.micronaut.pulsar.conf.ClientConf
import io.micronaut.pulsar.conf.StandaloneConf
import org.assertj.core.util.Files
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.Container
import org.testcontainers.containers.PulsarContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

final class PulsarTls implements AutoCloseable {

    public static final int HTTPS = 8443
    public static final int BROKER_SSL = 6651
    static final String caConfPath = "/my-ca";
    final PulsarContainer pulsarContainer
    final ClassLoader resourceLoader
    private final String PULSAR_CLI_ADMIN = "/pulsar/bin/pulsar-admin"

    PulsarTls() {
        this.resourceLoader = ClassLoader.getSystemClassLoader()
        this.pulsarContainer = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:2.8.0"))
    }

    void start() {
        String standalone = createStandaloneConfFile().path
        String client = createClientConf().path

        String brokerCert = resourceLoader.getResource("broker.cert.pem").path
        pulsarContainer.addFileSystemBind(new File(brokerCert).path, "$caConfPath/broker.cert.pem", BindMode.READ_ONLY)
        String brokerKey = resourceLoader.getResource("broker.key-pk8.pem").path
        pulsarContainer.addFileSystemBind(new File(brokerKey).path, "$caConfPath/broker.key-pk8.pem", BindMode.READ_ONLY)
        String caCert = resourceLoader.getResource("ca.cert.pem").path
        pulsarContainer.addFileSystemBind(new File(caCert).path, "$caConfPath/ca.cert.pem", BindMode.READ_ONLY)

        pulsarContainer.addFileSystemBind(standalone, "/pulsar/conf/standalone.conf", BindMode.READ_ONLY)
        pulsarContainer.addFileSystemBind(client, "/pulsar/conf/client.conf", BindMode.READ_ONLY)

        pulsarContainer.addExposedPorts(HTTPS, BROKER_SSL)
        pulsarContainer.start()
        createTopic()
    }

    String getPulsarBrokerUrl() {
        return String.format("pulsar+ssl://%s:%s", pulsarContainer.host, pulsarContainer.getMappedPort(BROKER_SSL));
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

    private void createTopic() {
        pulsarContainer.execInContainer("/bin/bash", "-c", PULSAR_CLI_ADMIN + " namespaces set-is-allow-auto-update-schema --enable public/default")
        Container.ExecResult result = pulsarContainer.execInContainer("/bin/bash", "-c", PULSAR_CLI_ADMIN + " topics create persistent://public/default/test")
        if (0 != result.exitCode) throw new RuntimeException("Unable to create test topic for TLS");
    }

    @Override
    void close() throws Exception {
        pulsarContainer.stop()
    }
}
