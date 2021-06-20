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
package example.shared

import org.assertj.core.util.Files
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.crypto.Algorithm
import org.keycloak.representations.idm.ClientRepresentation
import org.keycloak.representations.idm.ProtocolMapperRepresentation
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.Container
import org.testcontainers.containers.PulsarContainer
import org.testcontainers.containers.wait.strategy.WaitStrategy
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget
import org.testcontainers.images.builder.Transferable
import org.testcontainers.utility.DockerImageName

import java.time.Duration

/**
 * Share Pulsar Container class that ensures Pulsar is started after KeyCloak has all necessary configurations.
 *
 * @author Haris
 * @since 1.0
 */
final class SharedPulsar implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)

    private static final String CLIENT_CREDENTIALS = '''{
  "type": "client_credentials",
  "client_id": "pulsar-client",
  "client_secret": "%s",
  "issuer_url": "%s"
}'''
    private static final String caConfPath = "/my-ca"

    public static int SSL_PORT = 6651
    public static int HTTPS_PORT = 8443

    private final String PULSAR_CLI_CLIENT = "/pulsar/bin/pulsar-client"
    private final String PULSAR_CLI_ADMIN = "/pulsar/bin/pulsar-admin"

    private final SharedKeycloak keycloak
    private final PulsarContainer pulsarContainer
    private String credentialsPath

    SharedPulsar(SharedKeycloak keycloak) {
        this.keycloak = keycloak
        pulsarContainer = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:2.8.0"))
                .dependsOn(keycloak) // leave non-ssl ports for metrics test and such
        pulsarContainer.addExposedPorts(SSL_PORT, HTTPS_PORT)
    }

    String getUrl() {
        String brokerUrl = String.format("pulsar+ssl://%s:%s", pulsarContainer.getContainerIpAddress(),
                pulsarContainer.getMappedPort(SSL_PORT))
        return brokerUrl
    }

    String getCredentialsPath() {
        return credentialsPath
    }

    String getIssuerUrl() {
        return keycloak.crossContainerAuthUrl
    }

    void start() {
        setupTls()
        String secret = UUID.randomUUID().toString()
        credentialsPath = createCredentialsFile(secret).path

        RealmResource apiResource = keycloak.masterRealm
        createClient(apiResource, secret)

        File pubKey = generatePubKey(apiResource)
        // ensure all config files are deployed prior to run
        pulsarContainer.addFileSystemBind(credentialsPath, "/pulsar/credentials.json", BindMode.READ_ONLY)
        pulsarContainer.addFileSystemBind(pubKey.path, "/pulsar/pub.key", BindMode.READ_ONLY)
        Map<String, File> contentBytes = replaceConfigs(getIssuerUrl() + "/realms/master")
        pulsarContainer.addFileSystemBind(contentBytes["client"].path, "/pulsar/conf/client.conf", BindMode.READ_ONLY)
        pulsarContainer.addFileSystemBind(contentBytes["standalone"].path, "/pulsar/conf/standalone.conf", BindMode.READ_ONLY)

        pulsarContainer.start()
        createPrivateReports()
    }

    @Override
    void close() {
        pulsarContainer.stop()
    }

    Container.ExecResult send(String message) {
        pulsarContainer.copyFileToContainer(Transferable.of(message.bytes), "/pulsar/testMsg.json")
        String command = PULSAR_CLI_CLIENT + " produce persistent://public/default/messages -f testMsg.json";
        Container.ExecResult result = pulsarContainer.execInContainer("/bin/bash", "-c", command)
        if (0 != result.exitCode) {
            throw new Exception(result.stderr ?: result.stdout)
        }
        return result
    }

    private void createPrivateReports() {
        pulsarContainer.execInContainer("/bin/bash", "-c", PULSAR_CLI_ADMIN + " tenants create private -r superuser -c standalone")
        pulsarContainer.execInContainer("/bin/bash", "-c", PULSAR_CLI_ADMIN + " namespaces create private/reports")
        pulsarContainer.execInContainer("/bin/bash", "-c", PULSAR_CLI_ADMIN + " namespaces set-is-allow-auto-update-schema --enable private/reports")
        pulsarContainer.execInContainer("/bin/bash", "-c", PULSAR_CLI_ADMIN + " namespaces set-is-allow-auto-update-schema --enable public/default")

        pulsarContainer.execInContainer("/bin/bash", "-c", PULSAR_CLI_ADMIN + " topics create persistent://public/default/messages")
        pulsarContainer.execInContainer("/bin/bash", "-c", PULSAR_CLI_ADMIN + " topics create persistent://private/reports/messages")
    }

    private static File generatePubKey(RealmResource master) {
        String publicKey = master.keys().keyMetadata.keys.find {
            it.algorithm == Algorithm.RS256
        }.publicKey
        byte[] key = publicKey.decodeBase64()
        File f = Files.newTemporaryFile()
        f.bytes = key
        return f
    }

    private File createCredentialsFile(String secret) {
        String fileContents = String.format(CLIENT_CREDENTIALS, secret, keycloak.crossContainerAuthUrl + "/realms/master")
        File credentials = Files.newTemporaryFile()
        credentials.write(fileContents)
        return credentials
    }

    private static Map<String, File> replaceConfigs(String url) {
        String standaloneContent = StandaloneConf.content.replace("tlsCertificateFilePath=", "tlsCertificateFilePath=$caConfPath/broker.cert.pem")
        standaloneContent = standaloneContent.replace("tlsKeyFilePath=", "tlsKeyFilePath=$caConfPath/broker.key-pk8.pem")
        standaloneContent = standaloneContent.replace("tlsTrustCertsFilePath=", "tlsTrustCertsFilePath=$caConfPath/ca.cert.pem")
        standaloneContent = standaloneContent.replace('brokerClientAuthenticationParameters=',
                "brokerClientAuthenticationParameters={\"issuerUrl\": \"$url\",\"privateKey\": \"/pulsar/credentials.json\",\"audience\": \"pulsar\"}")
        File standalone = Files.newTemporaryFile()
        standalone.write(standaloneContent)

        String clientContent = ClientConf.content.replace("tlsTrustCertsFilePath=", "tlsTrustCertsFilePath=$caConfPath/ca.cert.pem")
        clientContent = clientContent.replace('authParams=',
                "authParams={\"issuerUrl\": \"$url\",\"privateKey\": \"file:///pulsar/credentials.json\",\"audience\": \"pulsar\"}")
        File client = Files.newTemporaryFile()
        client.write(clientContent)

        return ["client": client, "standalone": standalone]
    }

    private static void createClient(RealmResource master, String secret) {
        ClientRepresentation client = new ClientRepresentation(
                bearerOnly: false,
                clientId: "pulsar-client",
                directAccessGrantsEnabled: false, // password grant
                implicitFlowEnabled: false,
                standardFlowEnabled: false,
                serviceAccountsEnabled: true, // client credentials grant
                secret: secret,
                protocolMappers: [new ProtocolMapperRepresentation(
                        protocol: "openid-connect",
                        name: "role",
                        protocolMapper: "oidc-hardcoded-claim-mapper",
                        config: [
                                "claim.value"               : "superuser",
                                "userinfo.token.claim"      : "true",
                                "id.token.claim"            : "true",
                                "access.token.claim"        : "true",
                                "claim.name"                : "role",
                                "jsonType.label"            : "String",
                                "access.tokenResponse.claim": "false"
                        ])
                ])
        master.clients().create(client)
    }

    private void setupTls() {
        ClassLoader resourceLoader = ClassLoader.getSystemClassLoader()
        String brokerCert = resourceLoader.getResource("broker.cert.pem").path
        pulsarContainer.addFileSystemBind(new File(brokerCert).path, "$caConfPath/broker.cert.pem", BindMode.READ_ONLY)
        String brokerKey = resourceLoader.getResource("broker.key-pk8.pem").path
        pulsarContainer.addFileSystemBind(new File(brokerKey).path, "$caConfPath/broker.key-pk8.pem", BindMode.READ_ONLY)
        String caCert = resourceLoader.getResource("ca.cert.pem").path
        pulsarContainer.addFileSystemBind(new File(caCert).path, "$caConfPath/ca.cert.pem", BindMode.READ_ONLY)
    }
}
