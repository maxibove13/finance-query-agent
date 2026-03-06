"""Application settings from environment variables."""

from __future__ import annotations

import json
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    database_url: str | None = None  # asyncpg: postgresql://... (resolved from secret in Lambda)
    agent_model: str = "openai:gpt-4o"
    viz_model: str = "openai:gpt-4o-mini"
    dynamodb_table: str = "finance_agent_conversations"
    dynamodb_region: str = "us-east-1"
    schema_config_json: str | None = None  # inline JSON
    schema_config_path: str | None = None  # path to JSON file
    schema_config_ssm_param: str | None = None  # SSM parameter name (set via Lambda env var)
    encryption_key: str | None = None  # Fernet key (required in prod)
    logfire_token: str | None = None
    aws_lambda_function_name: str | None = None  # auto-set by Lambda

    # Agent execution limits (30s caller-side budget)
    request_budget: float = 28.0  # total wall time for entire request (Lambda=30s, leave 2s margin)
    agent_request_limit: int = 7
    agent_per_request_timeout: float = 12.0
    agent_run_timeout: float = 25.0

    # Secrets Manager ARNs (set via Terraform env vars in Lambda)
    db_credentials_secret_arn: str | None = None
    encryption_key_secret_arn: str | None = None
    llm_api_key_secret_arn: str | None = None
    logfire_token_secret_arn: str | None = None

    def resolve_secrets(self) -> None:
        """Fetch secrets from AWS Secrets Manager and SSM Parameter Store."""
        has_arns = any(
            [
                self.db_credentials_secret_arn,
                self.encryption_key_secret_arn,
                self.llm_api_key_secret_arn,
                self.logfire_token_secret_arn,
            ]
        )
        if has_arns:
            if self.db_credentials_secret_arn:
                raw = _resolve_secret(self.db_credentials_secret_arn)
                creds = json.loads(raw)
                self.database_url = (
                    f"postgresql://{creds['username']}:{creds['password']}"
                    f"@{creds['host']}:{creds.get('port', 5432)}/{creds['dbname']}"
                )

            if self.encryption_key_secret_arn:
                self.encryption_key = _resolve_secret(self.encryption_key_secret_arn)

            if self.llm_api_key_secret_arn:
                os.environ["OPENAI_API_KEY"] = _resolve_secret(self.llm_api_key_secret_arn)

            if self.logfire_token_secret_arn:
                os.environ["LOGFIRE_TOKEN"] = _resolve_secret(self.logfire_token_secret_arn)

            if not self.database_url:
                raise ValueError("database_url must be set directly or via db_credentials_secret_arn")

        # SSM-based config (independent of Secrets Manager)
        if self.schema_config_ssm_param and not self.schema_config_json:
            self.schema_config_json = _resolve_ssm_parameter(self.schema_config_ssm_param)


def _resolve_secret(arn: str) -> str:
    """Fetch a secret value from AWS Secrets Manager."""
    import boto3  # type: ignore[import-untyped]

    client = boto3.client("secretsmanager")
    resp = client.get_secret_value(SecretId=arn)
    return str(resp["SecretString"])


def _resolve_ssm_parameter(name: str) -> str:
    """Fetch a parameter value from AWS SSM Parameter Store."""
    import boto3

    client = boto3.client("ssm")
    resp = client.get_parameter(Name=name)
    return str(resp["Parameter"]["Value"])


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.resolve_secrets()
    return settings


def load_schema_json(settings: Settings) -> dict[str, Any]:
    """Load schema mapping JSON from settings (inline or file path)."""
    if settings.schema_config_json:
        result: dict[str, Any] = json.loads(settings.schema_config_json)
        return result
    if settings.schema_config_path:
        result = json.loads(Path(settings.schema_config_path).read_text())
        return result
    raise ValueError("Either schema_config_json or schema_config_path must be set")
