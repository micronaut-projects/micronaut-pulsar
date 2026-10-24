package io.micronaut.pulsar;

import java.io.IOException;

import org.apache.pulsar.client.api.CryptoKeyReader;

public interface LoadableCryptoKeyReader extends CryptoKeyReader {
    void load() throws IOException;
}
