"""Fernet field encryption with dev passthrough."""

from __future__ import annotations

import os

from cryptography.fernet import Fernet


class FieldEncryptor:
    """Encrypts/decrypts string fields. No-op in dev (no key), fail-hard in prod."""

    def __init__(self, key: str | None = None) -> None:
        self._fernet = Fernet(key.encode()) if key else None
        is_prod = os.environ.get("AWS_LAMBDA_FUNCTION_NAME") is not None
        if is_prod and not self._fernet:
            raise RuntimeError("ENCRYPTION_KEY required in production")

    def encrypt(self, plaintext: str) -> str:
        if not self._fernet:
            return plaintext
        return self._fernet.encrypt(plaintext.encode()).decode()

    def decrypt(self, ciphertext: str) -> str:
        if not self._fernet:
            return ciphertext
        return self._fernet.decrypt(ciphertext.encode()).decode()
