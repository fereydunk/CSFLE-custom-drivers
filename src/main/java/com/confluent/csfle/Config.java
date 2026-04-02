package com.confluent.csfle;

/**
 * Shared configuration — all values read from environment variables.
 *
 * Copy .env.example to .env, fill in your values, then export them:
 *   export $(grep -v '^#' .env | xargs)
 *
 * Or set them however your OS / IDE prefers.
 */
public class Config {

    // Kafka cluster
    public static final String BOOTSTRAP_SERVERS = require("KAFKA_BOOTSTRAP_SERVERS");
    public static final String KAFKA_API_KEY     = require("KAFKA_API_KEY");
    public static final String KAFKA_API_SECRET  = require("KAFKA_API_SECRET");

    // Schema Registry
    public static final String SR_URL            = require("SR_URL");
    public static final String SR_API_KEY        = require("SR_API_KEY");
    public static final String SR_API_SECRET     = require("SR_API_SECRET");

    // CipherTrust Manager
    public static final String CT_URL            = require("CT_URL");
    public static final String CT_USERNAME       = require("CT_USERNAME");
    public static final String CT_PASSWORD       = require("CT_PASSWORD");
    public static final String KEK_NAME          = require("CT_KEK_NAME");
    public static final String KMS_TYPE          = "ciphertrust-kms";
    public static final String KMS_KEY_ID        = ctHost() + "/keys/" + KEK_NAME;

    // Topics
    public static final String TOPIC             = "cluade-topic-ct";
    public static final String CONSUMER_GROUP    = "csfle-ct-java-consumer";
    public static final String JAVA_TOPIC        = "claude-test-ct-java";
    public static final String JAVA_CONSUMER_GROUP = "csfle-ct-java-demo-consumer";

    private Config() {}

    private static String require(String name) {
        String val = System.getenv(name);
        if (val == null || val.isBlank()) {
            throw new IllegalStateException("Required environment variable not set: " + name);
        }
        return val;
    }

    /** Extracts the host from CT_URL (strips the https:// prefix). */
    private static String ctHost() {
        return require("CT_URL").replaceFirst("^https?://", "");
    }
}
