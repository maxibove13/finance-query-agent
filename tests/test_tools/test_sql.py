"""Integration tests for execute_sql tool against a real Postgres container."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from finance_query_agent.tools import AgentDeps
from finance_query_agent.tools.sql import execute_sql


def _make_ctx(deps: AgentDeps) -> MagicMock:
    ctx = MagicMock()
    ctx.deps = deps
    return ctx


@pytest.mark.asyncio
async def test_select_returns_list_of_dicts(db_connection) -> None:
    deps = AgentDeps(connection=db_connection, user_id=1)
    ctx = _make_ctx(deps)

    rows = await execute_sql(ctx, "SELECT alias, currency FROM accounts ORDER BY id LIMIT 10")

    assert isinstance(rows, list)
    assert len(rows) == 2  # user 1 has 2 accounts (Checking, Savings UYU)
    assert rows[0]["alias"] == "Checking"
    assert rows[0]["currency"] == "USD"
    assert all(isinstance(r, dict) for r in rows)


@pytest.mark.asyncio
async def test_write_statement_raises_before_hitting_db(db_connection) -> None:
    deps = AgentDeps(connection=db_connection, user_id=1)
    ctx = _make_ctx(deps)

    with pytest.raises(ValueError, match="(?i)insert"):
        await execute_sql(ctx, "INSERT INTO accounts (user_id, currency) VALUES (1, 'USD')")

    # No tool call should have been recorded
    assert len(deps.tool_calls) == 0


@pytest.mark.asyncio
async def test_tool_call_is_recorded(db_connection) -> None:
    deps = AgentDeps(connection=db_connection, user_id=1)
    ctx = _make_ctx(deps)

    await execute_sql(ctx, "SELECT id FROM accounts ORDER BY id LIMIT 1")

    assert len(deps.tool_calls) == 1
    record = deps.tool_calls[0]
    assert record.tool_name == "execute_sql"
    assert "sql" in record.parameters
    assert record.row_count == 1
    assert record.execution_time_ms >= 0


@pytest.mark.asyncio
async def test_tool_result_is_recorded(db_connection) -> None:
    deps = AgentDeps(connection=db_connection, user_id=1)
    ctx = _make_ctx(deps)

    await execute_sql(ctx, "SELECT id FROM accounts ORDER BY id LIMIT 1")

    assert len(deps.tool_results) == 1
    name, data = deps.tool_results[0]
    assert name == "execute_sql"
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_user_isolation_account_movements(db_connection) -> None:
    """User 1 must not see user 2's account_movements row (RLS enforcement)."""
    deps = AgentDeps(connection=db_connection, user_id=1)
    ctx = _make_ctx(deps)

    rows = await execute_sql(ctx, "SELECT description FROM account_movements ORDER BY id LIMIT 100")
    descriptions = [r["description"] for r in rows]

    assert "Other User Groceries" not in descriptions
    assert "Whole Foods" in descriptions  # user 1's data is visible


@pytest.mark.asyncio
async def test_user_isolation_accounts(db_connection) -> None:
    """User 1 should only see their own accounts, not user 2's."""
    deps = AgentDeps(connection=db_connection, user_id=1)
    ctx = _make_ctx(deps)

    rows = await execute_sql(ctx, "SELECT alias FROM accounts ORDER BY id LIMIT 10")
    aliases = [r["alias"] for r in rows]

    assert "Other User" not in aliases
    assert "Checking" in aliases


@pytest.mark.asyncio
async def test_user2_isolation(db_connection) -> None:
    """User 2 sees only their own account and movement."""
    deps = AgentDeps(connection=db_connection, user_id=2)
    ctx = _make_ctx(deps)

    rows = await execute_sql(ctx, "SELECT description FROM account_movements ORDER BY id LIMIT 100")
    descriptions = [r["description"] for r in rows]

    assert "Other User Groceries" in descriptions
    assert "Whole Foods" not in descriptions
