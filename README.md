# CSFLE Custom Drivers

Custom KMS driver implementations for [Confluent Client-Side Field Level Encryption (CSFLE)](https://docs.confluent.io/platform/current/schema-registry/fundamentals/data-contracts.html#field-level-encryption).

Confluent CSFLE natively supports AWS KMS, Azure Key Vault, and GCP KMS. This repo shows how to plug in **any KMS** using the `KmsDriver` / `KmsClient` / `Aead` extension points — in both Java and Python.

## Structure

```
CSFLE-custom-drivers/
├── CipherTrust/          # Thales CipherTrust Manager
│   ├── java/
│   ├── python/
│   ├── .env.example      # Environment variable template for this provider
│   ├── INSTALL.md        # How to deploy CipherTrust Manager CE on Azure
│   └── README.md
├── .gitignore
└── README.md             # This file
```

Each top-level directory represents one KMS provider and is fully self-contained —
its own `.env.example`, installation guide, and implementation in each language.

## KMS Providers

| Provider | Java | Python | Install Guide |
|---|---|---|---|
| [CipherTrust Manager](./CipherTrust/README.md) | ✅ | ✅ | [Azure deployment](./CipherTrust/INSTALL.md) |

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

Fields tagged `PII` in the Avro schema are encrypted with a DEK. The DEK is wrapped
by the KMS KEK and stored in the Confluent DEK Registry. Decryption is transparent
on the consumer side.

## Getting started

Pick a KMS provider directory and follow its `README.md` and `INSTALL.md`.
Each provider directory contains its own `.env.example` — copy it, fill in your
values, and export them before running the producer or consumer.
