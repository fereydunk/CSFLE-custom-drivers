# CipherTrust — Python

Python implementation of the CipherTrust KMS driver for Confluent CSFLE.

## Structure

```
python/
├── ciphertrust_kms_driver.py   # Core driver — KmsDriver, KmsClient, Aead
├── config.py                   # All config from environment variables
├── producer.py                 # Registers schema, produces encrypted records
├── consumer.py                 # Consumes and transparently decrypts records
└── requirements.txt
```

## Prerequisites

- Python 3.8+
- Environment variables set (see `.env.example` in the repo root)

## Setup

```bash
# From repo root
cp .env.example .env
# Fill in your values, then export from repo root:
export $(grep -v '^#' .env | xargs)

# Then install dependencies
cd CipherTrust/python
pip install -r requirements.txt
```

## Run

Produce encrypted records:

```bash
python producer.py
```

Consume and decrypt:

```bash
python consumer.py
```

## Schema and encryption rule

The Avro schema tags `ssn` and `credit_card` as `PII`. A single CSFLE rule encrypts all `PII`-tagged fields using the CipherTrust KEK.
