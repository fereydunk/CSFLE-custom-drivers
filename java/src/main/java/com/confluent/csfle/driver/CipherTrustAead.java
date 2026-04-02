package com.confluent.csfle.driver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.crypto.tink.Aead;
import okhttp3.*;

import javax.net.ssl.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

/**
 * Tink Aead backed by CipherTrust Manager AES-256-GCM via REST API.
 *
 * Wire format of the ciphertext blob returned by encrypt():
 *   [4B iv_len][iv bytes][4B tag_len][tag bytes][ciphertext bytes]
 */
public class CipherTrustAead implements Aead {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    private static final long TOKEN_TTL_MS = 270_000L; // 4.5 min

    private final String baseUrl;
    private final String keyName;
    private final String username;
    private final String password;
    private final OkHttpClient http;

    private String token;
    private long tokenExpiryMs = 0;

    public CipherTrustAead(String baseUrl, String keyName, String username, String password) {
        this.baseUrl  = baseUrl.replaceAll("/$", "");
        this.keyName  = keyName;
        this.username = username;
        this.password = password;
        this.http     = buildTrustAllClient();
    }

    // ------------------------------------------------------------------
    // Aead interface
    // ------------------------------------------------------------------

    @Override
    public byte[] encrypt(byte[] plaintext, byte[] associatedData) throws GeneralSecurityException {
        ObjectNode body = MAPPER.createObjectNode();
        body.put("id",        keyName);
        body.put("plaintext", Base64.getEncoder().encodeToString(plaintext));
        body.put("mode",      "GCM");
        body.put("iv_length", 12);
        if (associatedData != null && associatedData.length > 0) {
            body.put("aad", Base64.getEncoder().encodeToString(associatedData));
        }

        try {
            JsonNode resp = post("/api/v1/crypto/encrypt", body);
            byte[] iv  = Base64.getDecoder().decode(resp.get("iv").asText());
            byte[] tag = Base64.getDecoder().decode(resp.get("tag").asText());
            byte[] ct  = Base64.getDecoder().decode(resp.get("ciphertext").asText());

            // Pack: [4B iv_len][iv][4B tag_len][tag][ciphertext]
            ByteBuffer buf = ByteBuffer.allocate(4 + iv.length + 4 + tag.length + ct.length);
            buf.putInt(iv.length);
            buf.put(iv);
            buf.putInt(tag.length);
            buf.put(tag);
            buf.put(ct);
            return buf.array();
        } catch (IOException e) {
            throw new GeneralSecurityException("CipherTrust encrypt failed", e);
        }
    }

    @Override
    public byte[] decrypt(byte[] ciphertext, byte[] associatedData) throws GeneralSecurityException {
        ByteBuffer buf = ByteBuffer.wrap(ciphertext);

        int ivLen = buf.getInt();
        byte[] iv = new byte[ivLen];
        buf.get(iv);

        int tagLen = buf.getInt();
        byte[] tag = new byte[tagLen];
        buf.get(tag);

        byte[] ct = new byte[buf.remaining()];
        buf.get(ct);

        ObjectNode body = MAPPER.createObjectNode();
        body.put("id",         keyName);
        body.put("ciphertext", Base64.getEncoder().encodeToString(ct));
        body.put("tag",        Base64.getEncoder().encodeToString(tag));
        body.put("mode",       "GCM");
        body.put("iv",         Base64.getEncoder().encodeToString(iv));
        if (associatedData != null && associatedData.length > 0) {
            body.put("aad", Base64.getEncoder().encodeToString(associatedData));
        }

        try {
            JsonNode resp = post("/api/v1/crypto/decrypt", body);
            return Base64.getDecoder().decode(resp.get("plaintext").asText());
        } catch (IOException e) {
            throw new GeneralSecurityException("CipherTrust decrypt failed", e);
        }
    }

    // ------------------------------------------------------------------
    // Token management
    // ------------------------------------------------------------------

    private synchronized String getToken() throws IOException {
        if (token != null && System.currentTimeMillis() < tokenExpiryMs) {
            return token;
        }
        ObjectNode body = MAPPER.createObjectNode();
        body.put("grant_type", "password");
        body.put("username",   username);
        body.put("password",   password);

        Request req = new Request.Builder()
                .url(baseUrl + "/api/v1/auth/tokens")
                .addHeader("Accept", "application/json")
                .post(RequestBody.create(MAPPER.writeValueAsBytes(body), JSON))
                .build();

        try (Response resp = http.newCall(req).execute()) {
            JsonNode json = MAPPER.readTree(resp.body().bytes());
            token = json.get("jwt").asText();
            tokenExpiryMs = System.currentTimeMillis() + TOKEN_TTL_MS;
            return token;
        }
    }

    // ------------------------------------------------------------------
    // HTTP helpers
    // ------------------------------------------------------------------

    private JsonNode post(String path, ObjectNode body) throws IOException {
        byte[] bytes = rawPost(path, body);
        return MAPPER.readTree(bytes);
    }

    private byte[] rawPost(String path, ObjectNode body) throws IOException {
        Request req = new Request.Builder()
                .url(baseUrl + path)
                .addHeader("Authorization", "Bearer " + getToken())
                .addHeader("Accept", "application/json")
                .post(RequestBody.create(MAPPER.writeValueAsBytes(body), JSON))
                .build();
        try (Response resp = http.newCall(req).execute()) {
            if (!resp.isSuccessful()) {
                throw new IOException("CipherTrust API error " + resp.code() + ": " + resp.body().string());
            }
            return resp.body().bytes();
        }
    }

    // ------------------------------------------------------------------
    // Trust-all TLS client (self-signed cert on CE)
    // ------------------------------------------------------------------

    private static OkHttpClient buildTrustAllClient() {
        try {
            TrustManager[] tm = {new X509TrustManager() {
                public void checkClientTrusted(X509Certificate[] c, String a) {}
                public void checkServerTrusted(X509Certificate[] c, String a) {}
                public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
            }};
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, tm, new java.security.SecureRandom());
            return new OkHttpClient.Builder()
                    .sslSocketFactory(ctx.getSocketFactory(), (X509TrustManager) tm[0])
                    .hostnameVerifier((h, s) -> true)
                    .connectTimeout(15, TimeUnit.SECONDS)
                    .readTimeout(15, TimeUnit.SECONDS)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to build HTTP client", e);
        }
    }
}
