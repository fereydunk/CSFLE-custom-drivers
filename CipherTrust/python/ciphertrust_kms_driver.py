"""
Custom CipherTrust Manager KMS Driver for Confluent CSFLE.

Implements the KmsDriver / KmsClient / Aead interfaces required by
confluent-kafka's schema registry encryption framework, using the
CipherTrust Manager REST API for AES-256-GCM key wrap/unwrap.

Key URL format: ciphertrust-kms://<ct_host>/keys/<key_name>
Example:        ciphertrust-kms://192.168.1.10/keys/my-aes256-key
"""

import base64
import json
import struct
import time
from typing import Any, Dict, Optional

import requests
import tink
from tink import KmsClient, aead

urllib3_logger = __import__("logging").getLogger("urllib3")
urllib3_logger.setLevel(__import__("logging").WARNING)

_PREFIX = "ciphertrust-kms://"
_CT_URL_CONF = "ciphertrust.url"
_CT_USERNAME_CONF = "ciphertrust.username"
_CT_PASSWORD_CONF = "ciphertrust.password"
_TOKEN_TTL_SECONDS = 270  # refresh before 5-min expiry


class CipherTrustAead(aead.Aead):
    """
    Tink Aead implementation backed by CipherTrust Manager AES-256-GCM.

    Wire format of the returned ciphertext blob:
        [4B iv_len][iv][4B tag_len][tag][ciphertext]
    """

    def __init__(self, base_url: str, key_name: str, username: str, password: str):
        self._base_url = base_url.rstrip("/")
        self._key_name = key_name
        self._username = username
        self._password = password
        self._token: Optional[str] = None
        self._token_expiry: float = 0.0
        self._session = requests.Session()
        self._session.verify = False  # self-signed cert on CE

    # ------------------------------------------------------------------
    # Token management
    # ------------------------------------------------------------------

    def _get_token(self) -> str:
        if self._token and time.time() < self._token_expiry:
            return self._token
        resp = self._session.post(
            f"{self._base_url}/api/v1/auth/tokens",
            json={"grant_type": "password",
                  "username": self._username,
                  "password": self._password},
            timeout=15,
        )
        resp.raise_for_status()
        self._token = resp.json()["jwt"]
        self._token_expiry = time.time() + _TOKEN_TTL_SECONDS
        return self._token

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._get_token()}",
                "Content-Type": "application/json"}

    # ------------------------------------------------------------------
    # Aead interface
    # ------------------------------------------------------------------

    def encrypt(self, plaintext: bytes, associated_data: bytes) -> bytes:
        """Encrypt plaintext with AES-256-GCM using the CipherTrust KEK."""
        payload = {
            "id": self._key_name,
            "plaintext": base64.b64encode(plaintext).decode(),
            "mode": "GCM",
            "iv_length": 12,
        }
        if associated_data:
            payload["aad"] = base64.b64encode(associated_data).decode()

        resp = self._session.post(
            f"{self._base_url}/api/v1/crypto/encrypt",
            headers=self._headers(),
            json=payload,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        iv = base64.b64decode(data["iv"])
        tag = base64.b64decode(data["tag"])
        ct = base64.b64decode(data["ciphertext"])

        # Pack: [4B iv_len][iv][4B tag_len][tag][ciphertext]
        return (struct.pack(">I", len(iv)) + iv +
                struct.pack(">I", len(tag)) + tag +
                ct)

    def decrypt(self, ciphertext: bytes, associated_data: bytes) -> bytes:
        """Decrypt ciphertext with AES-256-GCM using the CipherTrust KEK."""
        offset = 0
        iv_len = struct.unpack_from(">I", ciphertext, offset)[0]
        offset += 4
        iv = ciphertext[offset:offset + iv_len]
        offset += iv_len

        tag_len = struct.unpack_from(">I", ciphertext, offset)[0]
        offset += 4
        tag = ciphertext[offset:offset + tag_len]
        offset += tag_len

        ct = ciphertext[offset:]

        payload = {
            "id": self._key_name,
            "ciphertext": base64.b64encode(ct).decode(),
            "tag": base64.b64encode(tag).decode(),
            "mode": "GCM",
            "iv": base64.b64encode(iv).decode(),
        }
        if associated_data:
            payload["aad"] = base64.b64encode(associated_data).decode()

        resp = self._session.post(
            f"{self._base_url}/api/v1/crypto/decrypt",
            headers=self._headers(),
            json=payload,
            timeout=15,
        )
        resp.raise_for_status()
        # CipherTrust returns raw plaintext (not JSON)
        return resp.content


class CipherTrustKmsClient(KmsClient):
    """KmsClient that creates CipherTrustAead instances per key URI."""

    def __init__(self, username: str, password: str):
        self._username = username
        self._password = password

    def does_support(self, key_uri: str) -> bool:
        return key_uri.startswith(_PREFIX)

    def get_aead(self, key_uri: str) -> aead.Aead:
        # key_uri: ciphertrust-kms://<host>/keys/<key_name>
        if not key_uri.startswith(_PREFIX):
            raise tink.TinkError(f"Unsupported key URI: {key_uri}")
        rest = key_uri[len(_PREFIX):]          # "<host>/keys/<key_name>"
        parts = rest.split("/keys/", 1)
        if len(parts) != 2:
            raise tink.TinkError(f"Invalid CipherTrust key URI: {key_uri}")
        host, key_name = parts
        base_url = f"https://{host}"
        return CipherTrustAead(base_url, key_name, self._username, self._password)


class CipherTrustKmsDriver:
    """
    Confluent KmsDriver for CipherTrust Manager.

    Credentials are baked in at registration time so no extra conf keys
    are needed on the AvroSerializer.

    Register once at startup:
        CipherTrustKmsDriver.register(username="admin", password="...")

    Then reference KEKs with:
        kek_url = "ciphertrust-kms://192.168.1.10/keys/my-aes256-key"
    """

    def __init__(self, username: str, password: str) -> None:
        self._username = username
        self._password = password

    def get_key_url_prefix(self) -> str:
        return _PREFIX

    def new_kms_client(self, conf: Dict[str, Any], key_url: Optional[str]) -> KmsClient:
        # Prefer creds from conf if explicitly provided, fall back to baked-in creds
        username = conf.get(_CT_USERNAME_CONF, self._username)
        password = conf.get(_CT_PASSWORD_CONF, self._password)
        return CipherTrustKmsClient(username, password)

    @classmethod
    def register(cls, username: str, password: str) -> None:
        from confluent_kafka.schema_registry.rules.encryption.kms_driver_registry import register_kms_driver
        register_kms_driver(cls(username=username, password=password))
