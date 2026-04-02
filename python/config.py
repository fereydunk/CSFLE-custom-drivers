"""
Shared configuration for producer and consumer.

Copy .env.example to .env, fill in your values, then load them:
    export $(grep -v '^#' .env | xargs)
"""

import os

def _require(name):
    val = os.environ.get(name)
    if not val:
        raise EnvironmentError(f"Required environment variable not set: {name}")
    return val

KAFKA_CONFIG = {
    "bootstrap.servers": _require("KAFKA_BOOTSTRAP_SERVERS"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": _require("KAFKA_API_KEY"),
    "sasl.password": _require("KAFKA_API_SECRET"),
}

SR_CONFIG = {
    "url": _require("SR_URL"),
    "basic.auth.user.info": f"{_require('SR_API_KEY')}:{_require('SR_API_SECRET')}",
}

CIPHERTRUST_CONFIG = {
    "ciphertrust.url": _require("CT_URL"),
    "ciphertrust.username": _require("CT_USERNAME"),
    "ciphertrust.password": _require("CT_PASSWORD"),
}

TOPIC = "cluade-topic-ct"
KEK_NAME = _require("CT_KEK_NAME")
KEK_URL = f"ciphertrust-kms://{_require('CT_URL').replace('https://', '')}/keys/{KEK_NAME}"
