package com.confluent.csfle.driver;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KmsClient;

import java.security.GeneralSecurityException;

/**
 * Tink KmsClient that creates a CipherTrustAead for each key URI.
 *
 * Key URI format: ciphertrust-kms://<host>/keys/<key_name>
 */
public class CipherTrustKmsClient implements KmsClient {

    static final String PREFIX = "ciphertrust-kms://";

    private final String username;
    private final String password;

    public CipherTrustKmsClient(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public boolean doesSupport(String keyUri) {
        return keyUri.startsWith(PREFIX);
    }

    @Override
    public KmsClient withCredentials(String credentialPath) {
        return this; // credentials are baked in
    }

    @Override
    public KmsClient withDefaultCredentials() {
        return this;
    }

    @Override
    public Aead getAead(String keyUri) throws GeneralSecurityException {
        if (!doesSupport(keyUri)) {
            throw new GeneralSecurityException("Unsupported key URI: " + keyUri);
        }
        // keyUri = "ciphertrust-kms://<host>/keys/<key_name>"
        String rest = keyUri.substring(PREFIX.length());  // "<host>/keys/<key_name>"
        String[] parts = rest.split("/keys/", 2);
        if (parts.length != 2) {
            throw new GeneralSecurityException("Invalid CipherTrust key URI: " + keyUri);
        }
        String host    = parts[0];
        String keyName = parts[1];
        return new CipherTrustAead("https://" + host, keyName, username, password);
    }
}
