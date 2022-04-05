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
package example.java.shared;

import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.resource.RealmResource;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Objects;

/**
 * Share KeyCloak generic container for simulating SSO server providing OAuth2 to both Pulsar and Micronaut app.
 *
 * @author Haris
 * @since 1.0
 */
public class SharedKeycloak extends GenericContainer<SharedKeycloak> {

    private static final String USERNAME = "admin";
    private static final String PASSWORD = "admin";
    private static final int HTTP_PORT = 8080;
    public static final String KEYCLOAK_IMAGE = "quay.io/keycloak/keycloak";
    public static final String KEYCLOAK_VERSION = "16.1.0";
    public static final String REALM = "master";
    public static final String ADMIN_CLIENT_ID = "admin-cli";
    public static final String KEYCLOAK_IMAGE_NAME = String.format("%s:%s", KEYCLOAK_IMAGE, KEYCLOAK_VERSION);
    private Keycloak admin;

    public SharedKeycloak(DockerImageName dockerImageName) {
        super(Objects.requireNonNull(dockerImageName));
        withExposedPorts(8080); // ignore HTTPS we don't need it for test
        setWaitStrategy(Wait.forHttp("/auth").forPort(HTTP_PORT).withStartupTimeout(Duration.ofMinutes(5)));
    }

    @Override
    protected void configure() {
        withCommand("-c standalone.xml", "-b 0.0.0.0");
        withEnv("KEYCLOAK_USER", USERNAME);
        withEnv("KEYCLOAK_PASSWORD", PASSWORD);
    }

    public String getCrossContainerAuthUrl() {
        //Pulsar won't be able to use localhost for KeyCloak so additional step is required to ensure cross container
        //communication with throughout the same host but host itself won't see host.testcontainers.internal
        Testcontainers.exposeHostPorts(getPort());
        return String.format("http://%s:%d/auth", "host.testcontainers.internal", getPort());
    }

    public int getPort() {
        return getMappedPort(HTTP_PORT);
    }

    public Keycloak getAdmin() {
        if (null == this.admin) {
            this.admin = Keycloak.getInstance(String.format("http://%s:%d/auth", getHost(), getPort()), REALM, USERNAME, PASSWORD, ADMIN_CLIENT_ID);
        }
        return admin;
    }

    public RealmResource getMasterRealm() {
        return getAdmin().realm("master");
    }
}
