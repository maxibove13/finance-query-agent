"""Application settings from environment variables."""

from __future__ import annotations

import json
import logging
import os
from functools import lru_cache
from urllib.parse import quote_plus

from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    database_url: str | None = None  # asyncpg: postgresql://... (resolved from secret in Lambda)
    primary_model: str = "openai:gpt-4.1"
    secondary_model: str = "openai:gpt-4.1-mini"
    dynamodb_table: str = "finance_agent_conversations"
    dynamodb_region: str = "us-east-1"
    audit_table: str | None = None  # None = audit disabled
    encryption_key: str | None = None  # Fernet key (required in prod)
    logfire_token: str | None = None
    aws_lambda_function_name: str | None = None  # auto-set by Lambda

    # Input validation
    max_question_length: int = 2000  # characters
    max_session_id_length: int = 128

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
    semantic_model_ssm_path: str = "/mpi/finance-agent/semantic-model"

    def resolve_secrets(self) -> None:
        """Fetch secrets from AWS Secrets Manager."""
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
                    f"postgresql://{quote_plus(creds['username'])}:{quote_plus(creds['password'])}"
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


def _resolve_secret(arn: str) -> str:
    """Fetch a secret value from AWS Secrets Manager."""
    import boto3  # type: ignore[import-untyped]

    client = boto3.client("secretsmanager")
    try:
        resp = client.get_secret_value(SecretId=arn)
    except Exception:
        logger.error("Failed to resolve secret: %s", arn)
        raise
    return str(resp["SecretString"])


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.resolve_secrets()
    return settings
