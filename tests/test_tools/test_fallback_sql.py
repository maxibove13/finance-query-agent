"""Tests for the constrained SQL fallback tool."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from unittest.mock import AsyncMock

import pytest
from pydantic_ai import ModelRetry, RunContext

from finance_query_agent.schemas.mapping import (
    AmountConvention,
    ColumnRef,
    JoinDef,
    SchemaMapping,
    TableMapping,
)
from finance_query_agent.schemas.responses import ToolCallRecord
from finance_query_agent.tools.fallback_sql import (
    _strip_llm_user_filter,
    run_constrained_query,
)


@dataclass
class FakeDeps:
    """Minimal stand-in for AgentDeps — avoids importing QueryBuilder."""

    connection: Any
    schema: SchemaMapping
    user_id: str
    tool_calls: list[ToolCallRecord] = field(default_factory=list)
    fallback_used: bool = False
    fallback_sql: str | None = None
    query_builder: Any = None


@pytest.fixture()
def schema() -> SchemaMapping:
    return SchemaMapping(
        transactions=TableMapping(
            table="account_movements",
            columns={
                "date": "issued_at",
                "amount": "amount",
                "description": "description",
                "user_id": ColumnRef(table="accounts", column="user_id"),
                "currency": ColumnRef(table="accounts", column="currency"),
                "account_id": "account_id",
            },
            joins=[
                JoinDef(table="accounts", on="account_movements.account_id = accounts.id", type="inner"),
                JoinDef(table="tags", on="account_movements.category_id = tags.id", type="left"),
            ],
            amount_convention=AmountConvention(
                direction_column="movement_direction",
                expense_value="debit",
                income_value="credit",
            ),
        ),
        categories=TableMapping(
            table="tags",
            columns={"id": "id", "name": "name"},
            user_scoped=False,
        ),
        accounts=TableMapping(
            table="accounts",
            columns={"id": "id", "name": "alias", "user_id": "user_id"},
        ),
    )


class FakeRecord(dict):
    """Dict subclass that mimics asyncpg.Record for dict(row) conversion."""


def _make_ctx(deps: FakeDeps) -> RunContext[FakeDeps]:
    """Create a minimal RunContext-like object for testing."""
    # RunContext is a dataclass; we create a fake that has .deps
    ctx = type("FakeRunContext", (), {"deps": deps})()  # type: ignore[return-value]
    return ctx  # type: ignore[return-value]


def _make_connection(
    fetch_return: list[dict[str, Any]] | None = None,
    fetch_side_effect: Exception | None = None,
) -> AsyncMock:
    conn = AsyncMock()
    if fetch_side_effect:
        conn.fetch.side_effect = fetch_side_effect
    elif fetch_return is not None:
        records = [FakeRecord(r) for r in fetch_return]
        # First call is EXPLAIN, second is actual query
        conn.fetch.side_effect = [records, records]
    else:
        conn.fetch.return_value = []
    return conn


class TestValidationRejection:
    @pytest.mark.asyncio()
    async def test_rejects_dml(self, schema: SchemaMapping) -> None:
        conn = _make_connection()
        deps = FakeDeps(connection=conn, schema=schema, user_id="u1")
        ctx = _make_ctx(deps)

        with pytest.raises(ModelRetry, match="Forbidden keyword"):
            await run_constrained_query(ctx, "DELETE FROM account_movements")

    @pytest.mark.asyncio()
    async def test_rejects_unmapped_table(self, schema: SchemaMapping) -> None:
        conn = _make_connection()
        deps = FakeDeps(connection=conn, schema=schema, user_id="u1")
        ctx = _make_ctx(deps)

        with pytest.raises(ModelRetry, match="Table not in allowlist"):
            await run_constrained_query(ctx, "SELECT * FROM users")

    @pytest.mark.asyncio()
    async def test_rejects_cte(self, schema: SchemaMapping) -> None:
        conn = _make_connection()
        deps = FakeDeps(connection=conn, schema=schema, user_id="u1")
        ctx = _make_ctx(deps)

        with pytest.raises(ModelRetry, match="CTE"):
            await run_constrained_query(ctx, "WITH t AS (SELECT 1) SELECT * FROM t")

    @pytest.mark.asyncio()
    async def test_rejects_subquery(self, schema: SchemaMapping) -> None:
        conn = _make_connection()
        deps = FakeDeps(connection=conn, schema=schema, user_id="u1")
        ctx = _make_ctx(deps)

        with pytest.raises(ModelRetry, match="Subquer"):
            await run_constrained_query(
                ctx,
                "SELECT amount FROM account_movements WHERE amount > (SELECT 1)",
            )


class TestLimitInjection:
    @pytest.mark.asyncio()
    async def test_injects_limit_when_missing(self, schema: SchemaMapping) -> None:
        records = [FakeRecord({"amount": 100})]
        conn = AsyncMock()
        conn.fetch.side_effect = [records, records]  # EXPLAIN, then query

        deps = FakeDeps(connection=conn, schema=schema, user_id="u1")
        ctx = _make_ctx(deps)

        await run_constrained_query(ctx, "SELECT amount FROM account_movements")

        # The actual query (second call) should contain LIMIT
        actual_sql = conn.fetch.call_args_list[1][0][0]
        assert "LIMIT" in actual_sql


class TestUserFilterInjection:
    @pytest.mark.asyncio()
    async def test_injects_user_filter(self, schema: SchemaMapping) -> None:
        records = [FakeRecord({"amount": 100})]
        conn = AsyncMock()
        conn.fetch.side_effect = [records, records]

        deps = FakeDeps(connection=conn, schema=schema, user_id="u1")
        ctx = _make_ctx(deps)

        await run_constrained_query(ctx, "SELECT amount FROM account_movements")

        actual_sql = conn.fetch.call_args_list[1][0][0]
        assert "accounts.user_id = $1" in actual_sql

    @pytest.mark.asyncio()
    async def test_strips_llm_user_filter(self, schema: SchemaMapping) -> None:
        records = [FakeRecord({"amount": 100})]
        conn = AsyncMock()
        conn.fetch.side_effect = [records, records]

        deps = FakeDeps(connection=conn, schema=schema, user_id="u1")
        ctx = _make_ctx(deps)

        # LLM incorrectly added user_id filter — SDK should strip and replace
        sql = "SELECT amount FROM account_movements WHERE accounts.user_id = $1 AND amount > 0"
        await run_constrained_query(ctx, sql)

        actual_sql = conn.fetch.call_args_list[1][0][0]
        # Should have exactly one user_id condition (SDK-injected)
        assert actual_sql.count("user_id") == 1


class TestExplainValidation:
    @pytest.mark.asyncio()
    async def test_raises_model_retry_on_explain_failure(self, schema: SchemaMapping) -> None:
        conn = AsyncMock()
        conn.fetch.side_effect = Exception("column does_not_exist does not exist")

        deps = FakeDeps(connection=conn, schema=schema, user_id="u1")
        ctx = _make_ctx(deps)

        with pytest.raises(ModelRetry, match="EXPLAIN failed"):
            await run_constrained_query(ctx, "SELECT amount FROM account_movements")


class TestSuccessfulExecution:
    @pytest.mark.asyncio()
    async def test_returns_results_as_dicts(self, schema: SchemaMapping) -> None:
        records = [FakeRecord({"amount": 100, "description": "Netflix"})]
        conn = AsyncMock()
        conn.fetch.side_effect = [[], records]  # EXPLAIN returns empty, query returns data

        deps = FakeDeps(connection=conn, schema=schema, user_id="u1")
        ctx = _make_ctx(deps)

        result = await run_constrained_query(ctx, "SELECT amount, description FROM account_movements")

        assert result == [{"amount": 100, "description": "Netflix"}]

    @pytest.mark.asyncio()
    async def test_sets_fallback_metadata(self, schema: SchemaMapping) -> None:
        conn = AsyncMock()
        conn.fetch.side_effect = [[], []]

        deps = FakeDeps(connection=conn, schema=schema, user_id="u1")
        ctx = _make_ctx(deps)

        await run_constrained_query(ctx, "SELECT amount FROM account_movements")

        assert deps.fallback_used is True
        assert deps.fallback_sql is not None
        assert len(deps.tool_calls) == 1
        assert deps.tool_calls[0].tool_name == "run_constrained_query"

    @pytest.mark.asyncio()
    async def test_records_row_count(self, schema: SchemaMapping) -> None:
        records = [FakeRecord({"a": 1}), FakeRecord({"a": 2})]
        conn = AsyncMock()
        conn.fetch.side_effect = [[], records]

        deps = FakeDeps(connection=conn, schema=schema, user_id="u1")
        ctx = _make_ctx(deps)

        await run_constrained_query(ctx, "SELECT amount FROM account_movements")

        assert deps.tool_calls[0].row_count == 2


class TestExecutionFailure:
    @pytest.mark.asyncio()
    async def test_raises_model_retry_on_execution_error(self, schema: SchemaMapping) -> None:
        conn = AsyncMock()
        # EXPLAIN succeeds, execution fails
        conn.fetch.side_effect = [[], Exception("connection lost")]

        deps = FakeDeps(connection=conn, schema=schema, user_id="u1")
        ctx = _make_ctx(deps)

        with pytest.raises(ModelRetry, match="Query execution failed"):
            await run_constrained_query(ctx, "SELECT amount FROM account_movements")


class TestStripLlmUserFilter:
    def test_strips_parameterized_filter(self) -> None:
        sql = "SELECT * FROM t WHERE user_id = $1 AND amount > 0"
        result = _strip_llm_user_filter(sql)
        assert "user_id" not in result
        assert "amount > 0" in result

    def test_strips_string_literal_filter(self) -> None:
        sql = "SELECT * FROM t WHERE accounts.user_id = 'abc' AND amount > 0"
        result = _strip_llm_user_filter(sql)
        assert "user_id" not in result
        assert "amount > 0" in result

    def test_cleans_dangling_where(self) -> None:
        sql = "SELECT * FROM t WHERE user_id = $1 ORDER BY amount"
        result = _strip_llm_user_filter(sql)
        assert "user_id" not in result
        assert "ORDER BY amount" in result
