# CipherTrust — CSFLE Custom Driver

Custom KMS driver implementations for [Thales CipherTrust Manager](https://cpl.thalesgroup.com/encryption/ciphertrust-manager).

## Structure

```
CipherTrust/
├── java/      # Java implementation (Maven)
├── python/    # Python implementation
└── README.md
```

## Prerequisites

- A running CipherTrust Manager instance (Community Edition or full) with an AES-256 key created
- A Confluent Cloud cluster with Schema Registry enabled
- Environment variables set (see `.env.example` in the repo root)

## Quick start

**Java** — see [java/](./java/) or jump straight to:
```bash
cd java
mvn clean package
java -cp target/csfle-ciphertrust-1.0-SNAPSHOT.jar com.confluent.csfle.producer.JavaDemoProducer
java -cp target/csfle-ciphertrust-1.0-SNAPSHOT.jar com.confluent.csfle.consumer.JavaDemoConsumer
```

**Python** — see [python/](./python/) or jump straight to:
```bash
cd python
pip install -r requirements.txt
python producer.py
python consumer.py
```

## How the driver works

The driver implements three Confluent/Tink interfaces:

| Class | Interface | Responsibility |
|---|---|---|
| `CipherTrustKmsDriver` | `KmsDriver` | Entry point — registered via ServiceLoader; matches `ciphertrust-kms://` URIs |
| `CipherTrustKmsClient` | `KmsClient` | Parses the key URI, creates the `Aead` |
| `CipherTrustAead` | `Aead` | Calls CipherTrust `/api/v1/crypto/encrypt` and `/decrypt` via REST |

The KEK URL format used in schema rules:
```
encrypt.kms.type   = ciphertrust-kms
encrypt.kms.key.id = <CT_HOST>/keys/<KEK_NAME>
```
