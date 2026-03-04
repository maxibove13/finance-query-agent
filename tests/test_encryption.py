"""Tests for Fernet field encryption."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from cryptography.fernet import Fernet

from finance_query_agent.encryption import FieldEncryptor


class TestFieldEncryptor:
    def test_round_trip_with_key(self):
        key = Fernet.generate_key().decode()
        enc = FieldEncryptor(key)
        plaintext = "sensitive data with numbers 12345678"
        ciphertext = enc.encrypt(plaintext)
        assert ciphertext != plaintext
        assert enc.decrypt(ciphertext) == plaintext

    def test_dev_passthrough_no_key(self):
        enc = FieldEncryptor(key=None)
        plaintext = "hello world"
        assert enc.encrypt(plaintext) == plaintext
        assert enc.decrypt(plaintext) == plaintext

    def test_prod_requires_key(self):
        with patch.dict(os.environ, {"AWS_LAMBDA_FUNCTION_NAME": "my-func"}):
            with pytest.raises(RuntimeError, match="ENCRYPTION_KEY required"):
                FieldEncryptor(key=None)

    def test_prod_with_key_works(self):
        key = Fernet.generate_key().decode()
        with patch.dict(os.environ, {"AWS_LAMBDA_FUNCTION_NAME": "my-func"}):
            enc = FieldEncryptor(key)
            ct = enc.encrypt("data")
            assert enc.decrypt(ct) == "data"

    def test_different_keys_fail(self):
        key1 = Fernet.generate_key().decode()
        key2 = Fernet.generate_key().decode()
        enc1 = FieldEncryptor(key1)
        enc2 = FieldEncryptor(key2)
        ct = enc1.encrypt("secret")
        with pytest.raises(Exception):
            enc2.decrypt(ct)

    def test_invalid_key_raises(self):
        with pytest.raises(Exception):
            FieldEncryptor("not-a-valid-fernet-key")
