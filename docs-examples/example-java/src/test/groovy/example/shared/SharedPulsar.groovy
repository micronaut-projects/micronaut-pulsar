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

import io.micronaut.core.type.Argument
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.RxHttpClient
import org.assertj.core.util.Files
import org.keycloak.admin.client.resource.RealmResource
import org.keycloak.crypto.Algorithm
import org.keycloak.representations.idm.ClientRepresentation
import org.keycloak.representations.idm.ProtocolMapperRepresentation
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.Container
import org.testcontainers.containers.PulsarContainer
import org.testcontainers.images.builder.Transferable
import org.testcontainers.utility.DockerImageName

/**
 * Share Pulsar Container class that ensures Pulsar is started after KeyCloak has all necessary configurations.
 *
 * @author Haris* @since 1.0
 */
final class SharedPulsar implements AutoCloseable {

    private static final String CLIENT_CREDENTIALS = '''{
  "type": "client_credentials",
  "client_id": "pulsar-client",
  "client_secret": "%s",
  "issuer_url": "%s"
}'''
    private final String PULSAR_CLI_CLIENT = "/pulsar/bin/pulsar-client";

    private final SharedKeycloak keycloak
    private final PulsarContainer pulsarContainer
    private String credentialsPath

    SharedPulsar(SharedKeycloak keycloak) {
        this.keycloak = keycloak
        pulsarContainer = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:2.7.0")).dependsOn(keycloak)
    }

    String getUrl() {
        return pulsarContainer.pulsarBrokerUrl
    }

    String getCredentialsPath() {
        return credentialsPath
    }

    String getIssuerUrl() {
        return keycloak.getCrossContainerAuthUrl().replace("host.testcontainers.internal", keycloak.getContainerIpAddress())
    }

    void start() {
        String secret = UUID.randomUUID().toString()
        credentialsPath = createCredentialsFile(secret).path

        RealmResource apiResource = keycloak.masterRealm
        createClient(apiResource, secret)

        File pubKey = generatePubKey(apiResource)
        // ensure all config files are deployed prior to run
        pulsarContainer.addFileSystemBind(credentialsPath, "/pulsar/credentials.json", BindMode.READ_ONLY)
        pulsarContainer.addFileSystemBind(pubKey.path, "/pulsar/pub.key", BindMode.READ_ONLY)
        Map<String, File> contentBytes = replaceConfigs(keycloak.crossContainerAuthUrl + "/realms/master")
        pulsarContainer.addFileSystemBind(contentBytes["client"].path, "/pulsar/conf/client.conf", BindMode.READ_ONLY)
        pulsarContainer.addFileSystemBind(contentBytes["standalone"].path, "/pulsar/conf/standalone.conf", BindMode.READ_ONLY)
        pulsarContainer.start()
        createPrivateReports()
    }

    void close() {
        pulsarContainer.stop()
    }

    Container.ExecResult send(String message) {
        pulsarContainer.copyFileToContainer(Transferable.of(message.bytes), "/pulsar/testMsg.json")
        return pulsarContainer.execInContainer("/bin/bash",
                "-c",
                PULSAR_CLI_CLIENT + " produce public/default/messages -f testMsg.json")
    }

    private void createPrivateReports() {
        RxHttpClient client = RxHttpClient.create(new URL(pulsarContainer.httpServiceUrl))
        HttpResponse<String> hasTenant = client.exchange(HttpRequest.GET("/admin/v2/tenants"), Argument.of(String.class))
                .blockingFirst()
        if (!hasTenant.body().contains("private")) {
            HttpRequest<?> tenant = HttpRequest.PUT("/admin/v2/tenants/private",
                    ["allowedClusters": ["standalone"], "allowedRoles": ["superuser"]]
            )
            if (HttpStatus.NO_CONTENT != client.exchange(tenant).blockingFirst().status()) {
                throw new RuntimeException("Could not create pulsar tenant")
            }
        }

        HttpResponse<String> hasNamespace = client.exchange(HttpRequest.GET("/admin/v2/namespaces/private"), Argument.of(String.class))
                .blockingFirst()
        if (!hasNamespace.body().contains("reports")) {
            HttpRequest<?> namespace = HttpRequest.PUT("/admin/v2/namespaces/private/reports", [:])
            if (HttpStatus.NO_CONTENT != client.exchange(namespace).blockingFirst().status()) {
                throw new RuntimeException("Could not create pulsar tenant")
            }
        }
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
        File client = replaceFileLine(ClientConf.content, 41, url)
        File standalone = replaceFileLine(StandaloneConf.content, 404, url)
        return ["client": client, "standalone": standalone]
    }

    private static File replaceFileLine(String content, int line, String param) {
        String[] text = content.split('\n')
        text[line] = String.format(text[line], param)
        File tmp = Files.newTemporaryFile()
        tmp.write(text.join("\n"))
        return tmp
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
}
