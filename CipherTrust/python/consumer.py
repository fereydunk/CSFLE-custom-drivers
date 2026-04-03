"""
CSFLE Consumer — decrypts sensitive fields using CipherTrust Manager as KMS.

Reads from cluade-topic-ct and transparently decrypts 'ssn' and 'credit_card'
fields using the CipherTrust KEK to unwrap the DEK stored in Schema Registry.
"""

import warnings
warnings.filterwarnings("ignore")

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.rules.encryption.encrypt_executor import FieldEncryptionExecutor
from confluent_kafka.schema_registry.rules.encryption.kms_driver_registry import register_kms_driver
from confluent_kafka.serialization import SerializationContext, MessageField

from ciphertrust_kms_driver import CipherTrustKmsDriver
from config import KAFKA_CONFIG, SR_CONFIG, CIPHERTRUST_CONFIG, TOPIC

# --------------------------------------------------------------------------
# Register CipherTrust KMS driver + encryption executor
# --------------------------------------------------------------------------
CipherTrustKmsDriver.register(
    username=CIPHERTRUST_CONFIG["ciphertrust.username"],
    password=CIPHERTRUST_CONFIG["ciphertrust.password"],
)
FieldEncryptionExecutor.register()

# --------------------------------------------------------------------------
# Schema Registry + Deserializer
# --------------------------------------------------------------------------
sr_client = SchemaRegistryClient(SR_CONFIG)

avro_deserializer = AvroDeserializer(
    sr_client,
    conf={"use.latest.version": True},
)

# --------------------------------------------------------------------------
# Consumer
# --------------------------------------------------------------------------
consumer_conf = {
    **KAFKA_CONFIG,
    "group.id": "csfle-ct-consumer-group",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC])

print(f"Consuming from '{TOPIC}' with CSFLE decryption via CipherTrust...\n")

ctx = SerializationContext(TOPIC, MessageField.VALUE)

try:
    while True:
        msg = consumer.poll(timeout=5.0)
        if msg is None:
            print("No more messages. Exiting.")
            break
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        user = avro_deserializer(msg.value(), ctx)
        print(f"Decrypted message: {user}")
finally:
    consumer.close()
