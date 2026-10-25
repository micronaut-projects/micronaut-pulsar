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
package io.micronaut.pulsar;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.pulsar.config.PulsarClientConfiguration;
import io.micronaut.pulsar.processor.DefaultEncryptionKeyReader;
import jakarta.inject.Singleton;

/**
 * Search for encryption keys in the resource classpath by default.
 * @see <a href="https://pulsar.apache.org/docs/4.0.x/security-encryption/">Pulsar security encryption</a>
 *
 * KECDSA key generation:
 * openssl ecparam -name secp521r1 -genkey -param_enc explicit -out test_ecdsa_privkey.pem
 * openssl ec -in test_ecdsa_privkey.pem -pubout -outform pem -out test_ecdsa_pubkey.pem
 */
@Factory
@Requires(beans = {PulsarClientConfiguration.class})
public class LoadableCryptoKeyReaderFactory {

    /**
     * @param pulsarClientConfiguration
     * @return a DefaultEncryptionKeyReader singleton instance.
     */
    @Singleton
    public LoadableCryptoKeyReader loadableCryptoKeyReader(final PulsarClientConfiguration pulsarClientConfiguration) {
        return new DefaultEncryptionKeyReader(
            pulsarClientConfiguration.getEncryptionPrivateKeyPath().orElse("classpath:ecdsa_privkey.pem"),
            pulsarClientConfiguration.getEncryptionPublicKeyPath().orElse("classpath:ecdsa_pubkey.pem"));
    }
}
