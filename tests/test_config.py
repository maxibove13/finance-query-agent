"""Tests for config.py — settings loading and secret resolution."""

from __future__ import annotations

import json

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
        assert s.llm_model == "openai:gpt-4o"
        assert s.dynamodb_table == "finance_agent_conversations"
        assert s.dynamodb_region == "us-east-1"
        assert s.database_url is None
        assert s.db_credentials_secret_arn is None


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
        # Should not raise or call boto3
        s.resolve_secrets()
