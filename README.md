# CSFLE Custom Drivers — CipherTrust + Confluent

A Java implementation of a custom KMS driver for [Confluent Client-Side Field Level Encryption (CSFLE)](https://docs.confluent.io/platform/current/schema-registry/fundamentals/data-contracts.html#field-level-encryption) backed by [Thales CipherTrust Manager](https://cpl.thalesgroup.com/encryption/ciphertrust-manager).

Confluent CSFLE natively supports AWS KMS, Azure Key Vault, and GCP KMS. This project shows how to plug in **any KMS** — CipherTrust in this case — using the `KmsDriver` / `KmsClient` / `Aead` extension points.

## How it works

```
Kafka Producer/Consumer
        │
        ▼
FieldEncryptionExecutor  (Confluent CSFLE rule engine)
        │
        ▼
KmsDriverManager  ──── loads ────▶  CipherTrustKmsDriver   (this project)
                                            │
                                            ▼
                                    CipherTrustKmsClient
                                            │
                                            ▼
                                    CipherTrustAead
                                     encrypt / decrypt
                                     via REST API
                                            │
                                            ▼
                                  CipherTrust Manager
                                  AES-256-GCM  (KEK)
```

Fields tagged `PII` in the Avro schema are encrypted with a DEK. The DEK itself is wrapped by the CipherTrust KEK and stored in the Confluent DEK Registry. Decryption is fully transparent on the consumer side.

## Project structure

```
src/main/java/com/confluent/csfle/
├── Config.java                          # All config from environment variables
├── driver/
│   ├── CipherTrustAead.java             # Tink Aead → CipherTrust /crypto/encrypt|decrypt
│   ├── CipherTrustKmsClient.java        # Tink KmsClient — parses ciphertrust-kms:// URIs
│   └── CipherTrustKmsDriver.java        # Confluent KmsDriver — registered via ServiceLoader
├── producer/
│   ├── JavaDemoProducer.java            # Creates topic, registers schema, produces 10 records
│   └── CipherTrustProducer.java        # Minimal producer example
└── consumer/
    ├── JavaDemoConsumer.java            # Consumes and decrypts 10 records
    └── CipherTrustConsumer.java        # Minimal consumer example

src/main/resources/
└── META-INF/services/
    └── io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver  # ServiceLoader registration
```

## Prerequisites

- Java 21+
- Maven 3.8+
- A running [CipherTrust Manager](https://cpl.thalesgroup.com/encryption/ciphertrust-manager) instance (CE or full) with an AES-256 key created
- A Confluent Cloud cluster with Schema Registry

## Setup

1. Copy the environment template and fill in your values:

```bash
cp .env.example .env
# edit .env with your credentials
export $(grep -v '^#' .env | xargs)
```

2. Build:

```bash
mvn clean package
```

3. Produce 10 encrypted records (also creates the topic and registers the schema):

```bash
java -cp target/csfle-ciphertrust-1.0-SNAPSHOT.jar \
  com.confluent.csfle.producer.JavaDemoProducer
```

4. Consume and decrypt:

```bash
java -cp target/csfle-ciphertrust-1.0-SNAPSHOT.jar \
  com.confluent.csfle.consumer.JavaDemoConsumer
```

## Environment variables

| Variable | Description |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Confluent Cloud bootstrap server (e.g. `xxx.confluent.cloud:9092`) |
| `KAFKA_API_KEY` | Kafka cluster API key |
| `KAFKA_API_SECRET` | Kafka cluster API secret |
| `SR_URL` | Schema Registry URL |
| `SR_API_KEY` | Schema Registry API key |
| `SR_API_SECRET` | Schema Registry API secret |
| `CT_URL` | CipherTrust Manager URL (e.g. `https://192.168.1.10`) |
| `CT_USERNAME` | CipherTrust admin username |
| `CT_PASSWORD` | CipherTrust admin password |
| `CT_KEK_NAME` | Name of the AES-256 key to use as KEK |

## Schema and encryption rule

The `Employee` schema tags `ssn`, `credit_card`, and `salary` as `PII`:

```json
{"name": "ssn", "type": "string", "confluent:tags": ["PII"]}
```

A single CSFLE rule covers all PII-tagged fields:

```java
new Rule("encryptPII", null, RuleKind.TRANSFORM, RuleMode.WRITEREAD,
         FieldEncryptionExecutor.TYPE, Set.of("PII"),
         Map.of("encrypt.kek.name",   "<KEK_NAME>",
                "encrypt.kms.type",   "ciphertrust-kms",
                "encrypt.kms.key.id", "<CT_HOST>/keys/<KEK_NAME>"),
         null, null, "ERROR,NONE", false)
```

## Key dependencies

```xml
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-schema-registry-client-encryption</artifactId>
    <version>7.9.0</version>
</dependency>
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-schema-registry-client-encryption-tink</artifactId>
    <version>7.9.0</version>
</dependency>
<dependency>
    <groupId>com.google.crypto.tink</groupId>
    <artifactId>tink</artifactId>
    <version>1.15.0</version>
</dependency>
```
