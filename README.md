# CSFLE Custom Drivers

Custom KMS driver implementations for [Confluent Client-Side Field Level Encryption (CSFLE)](https://docs.confluent.io/platform/current/schema-registry/fundamentals/data-contracts.html#field-level-encryption).

Confluent CSFLE natively supports AWS KMS, Azure Key Vault, and GCP KMS. This repo shows how to plug in **any KMS** using the `KmsDriver` / `KmsClient` / `Aead` extension points.

## Structure

```
CSFLE-custom-drivers/
├── CipherTrust/          # Thales CipherTrust Manager
│   ├── java/             # Java implementation
│   ├── python/           # Python implementation
│   └── README.md
├── .env.example          # Environment variable template (shared)
└── README.md             # This file
```

Each top-level directory represents a KMS provider. More will be added over time.

## KMS Providers

| Provider | Java | Python |
|---|---|---|
| [CipherTrust Manager](./CipherTrust/README.md) | ✅ | ✅ |

## How CSFLE field encryption works

```
Kafka Producer/Consumer
        │
        ▼
FieldEncryptionExecutor  (Confluent CSFLE rule engine)
        │
        ▼
KmsDriverManager  ──── loads ────▶  <Custom KmsDriver>
                                            │
                                            ▼
                                    <Custom KmsClient>
                                            │
                                            ▼
                                    <Custom Aead>
                                     encrypt / decrypt
                                     via KMS REST API
                                            │
                                            ▼
                                       KMS Provider
                                      (KEK: AES-256)
```

Fields tagged `PII` in the Avro schema are encrypted with a DEK. The DEK is wrapped by the KMS KEK and stored in the Confluent DEK Registry. Decryption is fully transparent on the consumer side.

## Environment variables

All implementations share the same environment variables. Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
export $(grep -v '^#' .env | xargs)
```

| Variable | Description |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Confluent Cloud bootstrap server |
| `KAFKA_API_KEY` | Kafka cluster API key |
| `KAFKA_API_SECRET` | Kafka cluster API secret |
| `SR_URL` | Schema Registry URL |
| `SR_API_KEY` | Schema Registry API key |
| `SR_API_SECRET` | Schema Registry API secret |
| `CT_URL` | CipherTrust Manager URL (e.g. `https://192.168.1.10`) |
| `CT_USERNAME` | CipherTrust admin username |
| `CT_PASSWORD` | CipherTrust admin password |
| `CT_KEK_NAME` | Name of the AES-256 key to use as KEK |
