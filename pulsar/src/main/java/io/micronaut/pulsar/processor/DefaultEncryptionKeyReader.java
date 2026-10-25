/*
 * Copyright 2017-2026 original authors
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
package io.micronaut.pulsar.processor;

import io.micronaut.pulsar.LoadableCryptoKeyReader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.pulsar.client.api.EncryptionKeyInfo;

/**
 * Single key reader to encrypt/decrypt pulsar messages.
 */
public class DefaultEncryptionKeyReader implements LoadableCryptoKeyReader {

    String privKeyPath;
    String pubKeyPath;
    volatile boolean loaded = false;

    EncryptionKeyInfo privateKeyInfo = new EncryptionKeyInfo();
    EncryptionKeyInfo publicKeyInfo = new EncryptionKeyInfo();

    public DefaultEncryptionKeyReader(String privKeyPath, String pubKeyPath) {
        this.privKeyPath = privKeyPath;
        this.pubKeyPath = pubKeyPath;
    }

    /**
     * Load ECDSA key from the classpath or from files.
     * @throws IOException
     */
    public synchronized void load() throws IOException {
        if (!loaded) {
            if (privKeyPath.startsWith("classpath:")) {
                privateKeyInfo.setKey(readResource(privKeyPath.substring("classpath:".length())));
            } else if (privKeyPath.startsWith("file://")) {
                privateKeyInfo.setKey(Files.readAllBytes(Paths.get(privKeyPath.substring("file://".length()))));
            } else {
                privateKeyInfo.setKey(Files.readAllBytes(Paths.get(privKeyPath)));
            }

            if (pubKeyPath.startsWith("classpath:")) {
                publicKeyInfo.setKey(readResource(pubKeyPath.substring("classpath:".length())));
            } else if (privKeyPath.startsWith("file://")) {
                publicKeyInfo.setKey(Files.readAllBytes(Paths.get(pubKeyPath.substring("file://".length()))));
            } else {
                publicKeyInfo.setKey(Files.readAllBytes(Paths.get(pubKeyPath)));
            }
            loaded = true;
        }
    }

    private byte[] readResource(String path) throws IOException {
        try (InputStream input = DefaultEncryptionKeyReader.class.getResourceAsStream(path)) {
            if (input == null) {
                throw new FileNotFoundException(path);
            }
            return input.readAllBytes();
        }
    }

    @Override
    public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
        return publicKeyInfo;
    }

    @Override
    public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> keyMeta) {
        return privateKeyInfo;
    }
}
