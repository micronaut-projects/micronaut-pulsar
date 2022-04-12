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
package example.java.shared

import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.Environment
import org.testcontainers.utility.DockerImageName
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

/**
 * Generic code required by all components to run prior application being able to run.
 *
 * @author Haris
 * @since 1.0
 */
abstract class SimulateEnv extends Specification {
    @Shared
    @AutoCleanup
    ApplicationContext context

    @Shared
    @AutoCleanup
    SharedKeycloak keycloak = new SharedKeycloak(DockerImageName.parse(SharedKeycloak.KEYCLOAK_IMAGE_NAME))

    @Shared
    @AutoCleanup
    SharedPulsar pulsar

    void setupSpec() {
        keycloak.start()
        pulsar = new SharedPulsar(keycloak)
        pulsar.start()
        String tlsPath = ClassLoader.getSystemClassLoader().getResource('ca.cert.pem').path
        String tlsPathForPulsar = new File(tlsPath).absolutePath
        context = ApplicationContext.run(['pulsar.service-url'          : pulsar.url,
                                          'pulsar.oauth-issuer-url'     : pulsar.getIssuerUrl(),
                                          'pulsar.oauth-credentials-url': "file:///" + pulsar.credentialsPath,
                                          'pulsar.tls-cert-file-path'   : tlsPathForPulsar,
                                          'pulsar.tls-ciphers'          : ['TLS_RSA_WITH_AES_256_GCM_SHA384', 'TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256'],
                                          'pulsar.tls-protocols'        : ['TLSv1.2', 'TLSv1.1'],
                                          'spec.name'                   : getClass().simpleName],
                Environment.TEST
        )
    }
}
