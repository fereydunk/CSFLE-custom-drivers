package com.confluent.csfle.driver;

import com.google.crypto.tink.KmsClient;
import io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver;

import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Optional;

/**
 * Confluent KmsDriver for CipherTrust Manager.
 *
 * Loaded automatically via ServiceLoader from:
 *   META-INF/services/io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver
 *
 * Credentials are passed via Kafka config keys:
 *   ciphertrust.username / ciphertrust.password
 *
 * KEK URL format used in schema rules:
 *   encrypt.kms.type   = "ciphertrust-kms"
 *   encrypt.kms.key.id = "20.3.104.215/keys/poc-aes256-key"
 */
public class CipherTrustKmsDriver implements KmsDriver {

    public static final String PREFIX = CipherTrustKmsClient.PREFIX;

    // No-arg constructor required by ServiceLoader
    public CipherTrustKmsDriver() {}

    @Override
    public String getKeyUrlPrefix() {
        return PREFIX;
    }

    @Override
    public KmsClient newKmsClient(Map<String, ?> config, Optional<String> keyUrl)
            throws GeneralSecurityException {
        String user = (String) config.get("ciphertrust.username");
        String pass = (String) config.get("ciphertrust.password");
        if (user == null || pass == null) {
            throw new GeneralSecurityException(
                    "ciphertrust.username and ciphertrust.password must be set in Kafka config");
        }
        return new CipherTrustKmsClient(user, pass);
    }

    /**
     * No-op: the driver is registered automatically by Confluent's KmsDriverManager
     * via ServiceLoader (META-INF/services/...KmsDriver).
     * Call this at startup to ensure the class is initialized before the first use.
     */
    public static void register(String username, String password) {
        // ServiceLoader registration is automatic — nothing to do here
    }
}
