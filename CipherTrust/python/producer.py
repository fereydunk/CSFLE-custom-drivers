"""
CSFLE Producer — encrypts sensitive fields using CipherTrust Manager as KMS.

Fields tagged PII (ssn) and FINANCIAL (credit_card) are encrypted at rest
using Confluent's field-level encryption. The KEK lives in CipherTrust;
DEKs are auto-generated, wrapped by the KEK, and stored in the DEK Registry.
"""

import warnings
warnings.filterwarnings("ignore")

import json
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.common.schema_registry_client import (
    Metadata, MetadataTags, RuleSet, Rule, RuleKind, RuleMode, RuleParams
)
from confluent_kafka.schema_registry.rules.encryption.encrypt_executor import FieldEncryptionExecutor
from confluent_kafka.schema_registry.rules.encryption.kms_driver_registry import register_kms_driver
from confluent_kafka.serialization import SerializationContext, MessageField

from ciphertrust_kms_driver import CipherTrustKmsDriver
from config import KAFKA_CONFIG, SR_CONFIG, CIPHERTRUST_CONFIG, TOPIC, KEK_NAME

# --------------------------------------------------------------------------
# Register CipherTrust KMS driver + encryption executor
# --------------------------------------------------------------------------
CipherTrustKmsDriver.register(
    username=CIPHERTRUST_CONFIG["ciphertrust.username"],
    password=CIPHERTRUST_CONFIG["ciphertrust.password"],
)
FieldEncryptionExecutor.register()

# --------------------------------------------------------------------------
# Avro schema — confluent:tags mark fields for encryption
# --------------------------------------------------------------------------
SCHEMA_STR = json.dumps({
    "type": "record",
    "name": "User",
    "namespace": "com.confluent.csfle.demo",
    "fields": [
        {"name": "name",        "type": "string"},
        {"name": "email",       "type": "string"},
        {"name": "ssn",         "type": "string", "confluent:tags": ["PII"]},
        {"name": "credit_card", "type": "string", "confluent:tags": ["PII"]},
    ],
})

# kms.key.id = everything after "ciphertrust-kms://"
KMS_KEY_ID = "20.3.104.215/keys/" + KEK_NAME

ruleset = RuleSet(
    migration_rules=None,
    domain_rules=[
        Rule(
            name="encryptPII",
            doc=None,
            kind=RuleKind.TRANSFORM,
            mode=RuleMode.WRITEREAD,
            type="ENCRYPT",
            tags=["PII"],
            params=RuleParams({
                "encrypt.kek.name": KEK_NAME,
                "encrypt.kms.type": "ciphertrust-kms",
                "encrypt.kms.key.id": KMS_KEY_ID,
            }),
            expr=None,
            on_success=None,
            on_failure="ERROR,NONE",
            disabled=False,
        ),
    ],
)

# --------------------------------------------------------------------------
# Register schema with encryption rules in Schema Registry
# --------------------------------------------------------------------------
sr_client = SchemaRegistryClient(SR_CONFIG)

schema = Schema(
    schema_str=SCHEMA_STR,
    schema_type="AVRO",
    rule_set=ruleset,
)

SUBJECT = f"{TOPIC}-value"
schema_id = sr_client.register_schema(SUBJECT, schema)
print(f"Schema registered: id={schema_id}, subject={SUBJECT}")

# --------------------------------------------------------------------------
# AvroSerializer — CipherTrust creds passed so driver can authenticate
# --------------------------------------------------------------------------
avro_serializer = AvroSerializer(
    schema_registry_client=sr_client,
    schema_str=SCHEMA_STR,
    conf={
        "auto.register.schemas": False,
        "use.latest.version": True,
    },
)

# --------------------------------------------------------------------------
# Producer
# --------------------------------------------------------------------------
producer = Producer(KAFKA_CONFIG)

USERS = [
    {"name": "Alice Smith",    "email": "alice@example.com",  "ssn": "123-45-6789", "credit_card": "4111-1111-1111-1111"},
    {"name": "Bob Johnson",    "email": "bob@example.com",    "ssn": "987-65-4321", "credit_card": "5500-0000-0000-0004"},
    {"name": "Carol Williams", "email": "carol@example.com",  "ssn": "555-12-3456", "credit_card": "3400-0000-0000-009"},
]


def delivery_report(err, msg):
    if err:
        print(f"  Delivery failed: {err}")
    else:
        print(f"  Delivered → partition={msg.partition()} offset={msg.offset()}")


ctx = SerializationContext(TOPIC, MessageField.VALUE)

print(f"\nProducing {len(USERS)} messages to '{TOPIC}' with CSFLE...\n")
for user in USERS:
    value = avro_serializer(user, ctx)
    producer.produce(TOPIC, value=value, on_delivery=delivery_report)
    print(f"  Produced (plaintext): name={user['name']} ssn={user['ssn']} cc={user['credit_card']}")

producer.flush()
print("\nDone — ssn and credit_card encrypted in Kafka using CipherTrust KEK.")
