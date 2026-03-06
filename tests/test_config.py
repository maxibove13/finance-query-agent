"""Tests for config.py — settings loading and secret resolution."""

from __future__ import annotations

import json
import os

import pytest

from finance_query_agent.config import Settings, load_schema_json


class TestLoadFromEnv:
    def test_loads_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost/db")
        monkeypatch.setenv("ENCRYPTION_KEY", "test-key")
        s = Settings()
        assert s.database_url == "postgresql://user:pass@localhost/db"
        assert s.encryption_key == "test-key"

    def test_defaults(self) -> None:
        s = Settings()
        assert s.agent_model == "openai:gpt-4o"
        assert s.dynamodb_table == "finance_agent_conversations"
        assert s.dynamodb_region == "us-east-1"
        assert s.database_url is None
        assert s.db_credentials_secret_arn is None
        assert s.agent_request_limit == 7
        assert s.agent_per_request_timeout == 12.0
        assert s.agent_run_timeout == 25.0


class TestLoadSchema:
    def test_load_schema_from_inline_json(self) -> None:
        data = {"transactions": {"table": "txns"}}
        s = Settings(schema_config_json=json.dumps(data))  # type: ignore[call-arg]
        result = load_schema_json(s)
        assert result == data

    def test_load_schema_from_file(self, tmp_path: pytest.TempPathFactory) -> None:
        data = {"transactions": {"table": "txns"}}
        p = tmp_path / "schema.json"  # type: ignore[operator]
        p.write_text(json.dumps(data))
        s = Settings(schema_config_path=str(p))  # type: ignore[call-arg]
        result = load_schema_json(s)
        assert result == data

    def test_load_schema_raises_when_neither_set(self) -> None:
        s = Settings()
        with pytest.raises(ValueError, match="schema_config_json or schema_config_path"):
            load_schema_json(s)


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


class TestResolveSSM:
    def test_ssm_param_populates_schema_config_json(self, monkeypatch: pytest.MonkeyPatch) -> None:
        schema_data = {"transactions": {"table": "txns"}}
        expected_json = json.dumps(schema_data)

        monkeypatch.setattr(
            "finance_query_agent.config._resolve_ssm_parameter",
            lambda name: expected_json,
        )

        s = Settings(schema_config_ssm_param="/test/schema-config")  # type: ignore[call-arg]
        s.resolve_secrets()
        assert s.schema_config_json == expected_json

    def test_env_var_takes_precedence_over_ssm(self) -> None:
        """When both schema_config_json and ssm_param are set, env var wins."""
        s = Settings(
            schema_config_json='{"already": "set"}',  # type: ignore[call-arg]
            schema_config_ssm_param="/test/schema-config",  # type: ignore[call-arg]
        )
        s.resolve_secrets()
        assert s.schema_config_json == '{"already": "set"}'

    def test_ssm_not_called_when_param_not_set(self) -> None:
        """When schema_config_ssm_param is None, no SSM call."""
        s = Settings()
        s.resolve_secrets()
