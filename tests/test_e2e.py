"""End-to-end tests: real Postgres (testcontainers), mocked DynamoDB (moto), real LLM.

Requires:
  - Docker running (for testcontainers Postgres)
  - OPENAI_API_KEY env var set (or whichever LLM provider you configure)

Run:
  uv run pytest tests/test_e2e.py -v
"""

from __future__ import annotations

import os

import pytest
from moto import mock_aws

from finance_query_agent.schemas.mapping import SchemaMapping

pytestmark = pytest.mark.skipif(
    not os.environ.get("OPENAI_API_KEY"),
    reason="OPENAI_API_KEY not set — skipping e2e tests",
)


@pytest.fixture(autouse=True)
def _reset_singletons():
    """Clear cached singletons so each test gets a fresh agent + settings."""
    import finance_query_agent.agent as agent_mod
    import finance_query_agent.config as config_mod
    import finance_query_agent.handler as handler_mod

    agent_mod._agent = None
    handler_mod._initialized = False
    config_mod.get_settings.cache_clear()
    yield
    agent_mod._agent = None
    handler_mod._initialized = False
    config_mod.get_settings.cache_clear()


@pytest.fixture
def _env(postgres_url: str, sample_schema_mapping: SchemaMapping):
    """Set env vars pointing at testcontainers Postgres + inline schema config."""
    schema_json = sample_schema_mapping.model_dump_json()
    env = {
        "DATABASE_URL": postgres_url,
        "SCHEMA_CONFIG_JSON": schema_json,
        "LLM_MODEL": "openai:gpt-4o-mini",
        "DYNAMODB_TABLE": "test-conversations",
        "DYNAMODB_REGION": "us-east-1",
    }
    old = {}
    for k, v in env.items():
        old[k] = os.environ.get(k)
        os.environ[k] = v
    yield
    for k, v in old.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def _make_event(user_id: str, session_id: str, question: str) -> dict:
    return {"user_id": user_id, "session_id": session_id, "question": question}


@mock_aws
def test_e2e_spending_question(_env, dynamodb_table):
    """Full handler invocation: real Postgres, mocked DynamoDB, real LLM."""
    from finance_query_agent.handler import handler

    result = handler(
        _make_event("test-user-1", "e2e-session-1", "How much did I spend on groceries in November 2025?"),
        None,
    )

    assert "error" not in result
    assert result["answer"]  # LLM returned something
    assert result["tool_calls"]  # at least one tool was called
    assert not result["unresolved"]


@mock_aws
def test_e2e_multi_currency(_env, dynamodb_table):
    """Verify multi-currency results come back (user has USD + UYU accounts)."""
    from finance_query_agent.handler import handler

    result = handler(
        _make_event("test-user-1", "e2e-session-2", "What were my total expenses in November 2025?"),
        None,
    )

    assert "error" not in result
    answer_lower = result["answer"].lower()
    assert "usd" in answer_lower or "uyu" in answer_lower


@mock_aws
def test_e2e_conversation_memory(_env, dynamodb_table):
    """Second question in the same session should have conversation context."""
    from finance_query_agent.handler import handler

    # First turn
    r1 = handler(
        _make_event("test-user-1", "e2e-session-3", "How much did I spend on groceries in October 2025?"),
        None,
    )
    assert "error" not in r1

    # Reset agent singleton (simulates new Lambda invocation, same warm container)
    import finance_query_agent.agent as agent_mod
    import finance_query_agent.config as config_mod
    import finance_query_agent.handler as handler_mod

    agent_mod._agent = None
    handler_mod._initialized = False
    config_mod.get_settings.cache_clear()

    # Second turn — same session, follow-up question
    r2 = handler(
        _make_event("test-user-1", "e2e-session-3", "And how about November?"),
        None,
    )
    assert "error" not in r2
    assert r2["tool_calls"]  # agent understood the follow-up and called a tool


@mock_aws
def test_e2e_user_isolation(_env, dynamodb_table):
    """test-user-2 should not see test-user-1 data."""
    from finance_query_agent.handler import handler

    result = handler(
        _make_event("test-user-2", "e2e-session-4", "Show me all my transactions in November 2025"),
        None,
    )

    assert "error" not in result
    # test-user-2 only has one transaction — "Other User Groceries"
    assert "whole foods" not in result["answer"].lower()


@mock_aws
def test_e2e_missing_field(_env, dynamodb_table):
    """Missing required field returns 400."""
    from finance_query_agent.handler import handler

    result = handler({"user_id": "x"}, None)
    assert "error" in result
