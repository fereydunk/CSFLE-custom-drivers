# CSFLE Custom Drivers — Python

Python implementation of the CipherTrust KMS driver for Confluent CSFLE.

## Files

| File | Purpose |
|---|---|
| `ciphertrust_kms_driver.py` | Core driver — `KmsDriver`, `KmsClient`, `Aead` backed by CipherTrust REST API |
| `config.py` | All config read from environment variables |
| `producer.py` | Registers schema with encryption rule, produces encrypted records |
| `consumer.py` | Consumes and transparently decrypts records |

## Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Set environment variables (same as Java — see `.env.example` in the repo root):

```bash
export $(grep -v '^#' ../.env | xargs)
```

3. Produce:

```bash
python producer.py
```

4. Consume:

```bash
python consumer.py
```
