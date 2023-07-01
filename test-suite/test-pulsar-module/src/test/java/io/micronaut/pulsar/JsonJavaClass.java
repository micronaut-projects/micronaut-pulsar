package io.micronaut.pulsar;

import io.micronaut.context.annotation.Requires;

@Requires(property = "spec.name", value = "PulsarConfigurationTest")
public class JsonJavaClass {
    private String id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
