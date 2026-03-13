"""Tests for config.py — settings loading and secret resolution."""

from __future__ import annotations

import json
import logging
import os
from unittest.mock import MagicMock, patch

import pytest

from finance_query_agent.config import Settings, _resolve_secret


class TestLoadFromEnv:
    def test_loads_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost/db")
        monkeypatch.setenv("ENCRYPTION_KEY", "test-key")
        s = Settings()
        assert s.database_url == "postgresql://user:pass@localhost/db"
        assert s.encryption_key == "test-key"

    def test_defaults(self) -> None:
        s = Settings()
        assert s.primary_model == "openai:gpt-4.1"
        assert s.secondary_model == "openai:gpt-4.1-mini"
        assert s.dynamodb_table == "finance_agent_conversations"
        assert s.dynamodb_region == "us-east-1"
        assert s.database_url is None
        assert s.db_credentials_secret_arn is None
        assert s.agent_request_limit == 7
        assert s.agent_per_request_timeout == 12.0
        assert s.agent_run_timeout == 25.0


class TestResolveSecrets:
    def test_resolve_secrets_skips_without_arns(self) -> None:
        """No ARNs set means no boto3 calls, no errors."""
        s = Settings()
        s.resolve_secrets()

    def test_resolve_db_credentials(self, monkeypatch: pytest.MonkeyPatch) -> None:
        creds = json.dumps(
            {"username": "ro", "password": "pw", "host": "db.example.com", "port": 5432, "dbname": "mydb"}
        )
        monkeypatch.setattr("finance_query_agent.config._resolve_secret", lambda arn: creds)

        s = Settings(db_credentials_secret_arn="arn:aws:secretsmanager:us-east-1:123:secret:db")  # type: ignore[call-arg]
        s.resolve_secrets()
        assert s.database_url == "postgresql://ro:pw@db.example.com:5432/mydb"

    def test_db_credentials_with_special_chars_encoded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Reserved characters in username/password must be percent-encoded."""
        creds = json.dumps(
            {
                "username": "ro@user",
                "password": "p@ss:word/db#1?x",
                "host": "db.example.com",
                "port": 5432,
                "dbname": "mydb",
            }
        )
        monkeypatch.setattr("finance_query_agent.config._resolve_secret", lambda arn: creds)

        s = Settings(db_credentials_secret_arn="arn:aws:secretsmanager:us-east-1:123:secret:db")  # type: ignore[call-arg]
        s.resolve_secrets()
        assert s.database_url is not None
        # Raw special chars must not appear in the authority section
        assert "@db.example.com" in s.database_url
        assert "ro%40user" in s.database_url
        assert "p%40ss%3Aword%2Fdb%231%3Fx" in s.database_url

    def test_resolve_encryption_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        creds = json.dumps({"username": "u", "password": "p", "host": "h", "port": 5432, "dbname": "d"})
        secrets = {"arn:db": creds, "arn:enc": "fernet-key-123"}
        monkeypatch.setattr("finance_query_agent.config._resolve_secret", lambda arn: secrets[arn])

        s = Settings(
            db_credentials_secret_arn="arn:db",  # type: ignore[call-arg]
            encryption_key_secret_arn="arn:enc",  # type: ignore[call-arg]
        )
        s.resolve_secrets()
        assert s.encryption_key == "fernet-key-123"

    def test_resolve_llm_api_key_sets_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("finance_query_agent.config._resolve_secret", lambda arn: "sk-test-key")

        s = Settings(
            llm_api_key_secret_arn="arn:llm",  # type: ignore[call-arg]
            database_url="postgresql://x:x@localhost/db",  # type: ignore[call-arg]
        )
        s.resolve_secrets()
        assert os.environ["OPENAI_API_KEY"] == "sk-test-key"

    def test_resolve_logfire_token_sets_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("finance_query_agent.config._resolve_secret", lambda arn: "lf-token")

        s = Settings(
            logfire_token_secret_arn="arn:logfire",  # type: ignore[call-arg]
            database_url="postgresql://x:x@localhost/db",  # type: ignore[call-arg]
        )
        s.resolve_secrets()
        assert os.environ["LOGFIRE_TOKEN"] == "lf-token"

    def test_raises_when_arns_set_but_no_database_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("finance_query_agent.config._resolve_secret", lambda arn: "some-key")

        s = Settings(encryption_key_secret_arn="arn:enc")  # type: ignore[call-arg]
        with pytest.raises(ValueError, match="database_url must be set"):
            s.resolve_secrets()


class TestResolveSecretLogging:
    def test_logs_on_secrets_manager_error(self, caplog: pytest.LogCaptureFixture) -> None:
        mock_client = MagicMock()
        mock_client.get_secret_value.side_effect = Exception("AccessDenied")
        with patch("boto3.client", return_value=mock_client):
            with caplog.at_level(logging.ERROR, logger="finance_query_agent.config"):
                with pytest.raises(Exception, match="AccessDenied"):
                    _resolve_secret("arn:aws:secretsmanager:us-east-1:123:secret:test")
        assert "Failed to resolve secret" in caplog.text
        assert "arn:aws:secretsmanager:us-east-1:123:secret:test" in caplog.text
