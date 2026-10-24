package io.micronaut.pulsar;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.pulsar.config.PulsarClientConfiguration;
import io.micronaut.pulsar.processor.DefaultEncryptionKeyReader;
import jakarta.inject.Singleton;

/**
 * Search for encryption keys in the resource classpath by default.
 * @see https://pulsar.apache.org/docs/4.0.x/security-encryption/
 *
 * KECDSA key generation:
 * openssl ecparam -name secp521r1 -genkey -param_enc explicit -out test_ecdsa_privkey.pem
 * openssl ec -in test_ecdsa_privkey.pem -pubout -outform pem -out test_ecdsa_pubkey.pem
 */
@Factory
@Requires(beans = {PulsarClientConfiguration.class})
public class LoadableCryptoKeyReaderFactory {

    @Singleton
    public LoadableCryptoKeyReader loadableCryptoKeyReader(final PulsarClientConfiguration pulsarClientConfiguration) {
        return new DefaultEncryptionKeyReader(
            pulsarClientConfiguration.getEncryptionPrivateKeyPath().orElse("classpath:ecdsa_privkey.pem"),
            pulsarClientConfiguration.getEncryptionPublicKeyPath().orElse("classpath:ecdsa_pubkey.pem"));
    }
}
