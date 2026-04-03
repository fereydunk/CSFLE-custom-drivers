# CipherTrust — Java

Java implementation of the CipherTrust KMS driver for Confluent CSFLE.

## Structure

```
java/
├── pom.xml
└── src/main/
    ├── java/com/confluent/csfle/
    │   ├── Config.java                     # All config from environment variables
    │   ├── driver/
    │   │   ├── CipherTrustAead.java        # Tink Aead → CipherTrust /crypto/encrypt|decrypt
    │   │   ├── CipherTrustKmsClient.java   # Tink KmsClient — parses ciphertrust-kms:// URIs
    │   │   └── CipherTrustKmsDriver.java   # Confluent KmsDriver — registered via ServiceLoader
    │   ├── producer/
    │   │   ├── JavaDemoProducer.java       # Creates topic, registers schema, produces 10 records
    │   │   └── CipherTrustProducer.java    # Minimal producer example
    │   └── consumer/
    │       ├── JavaDemoConsumer.java       # Consumes and decrypts records
    │       └── CipherTrustConsumer.java    # Minimal consumer example
    └── resources/
        └── META-INF/services/
            └── io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver
```

## Prerequisites

- Java 21+
- Maven 3.8+
- Environment variables set (see `.env.example` in the repo root)

## Setup

```bash
# From repo root
cp .env.example .env
# Fill in your values, then:
export $(grep -v '^#' .env | xargs)

cd CipherTrust/java
mvn clean package
```

## Run

Produce 10 encrypted records (also creates the topic and registers the schema):

```bash
java -cp target/csfle-ciphertrust-1.0-SNAPSHOT.jar \
  com.confluent.csfle.producer.JavaDemoProducer
```

Consume and decrypt:

```bash
java -cp target/csfle-ciphertrust-1.0-SNAPSHOT.jar \
  com.confluent.csfle.consumer.JavaDemoConsumer
```

## Schema and encryption rule

The `Employee` Avro schema tags `ssn`, `credit_card`, and `salary` as `PII`:

```json
{"name": "ssn", "type": "string", "confluent:tags": ["PII"]}
```

A single CSFLE rule encrypts all `PII`-tagged fields:

```java
new Rule("encryptPII", null, RuleKind.TRANSFORM, RuleMode.WRITEREAD,
         FieldEncryptionExecutor.TYPE, Set.of("PII"),
         Map.of("encrypt.kek.name",   "<CT_KEK_NAME>",
                "encrypt.kms.type",   "ciphertrust-kms",
                "encrypt.kms.key.id", "<CT_HOST>/keys/<CT_KEK_NAME>"),
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
