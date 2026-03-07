"""Tests for transaction tools — mock connection, verify row mapping and ToolCallRecord."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic_ai import RunContext

from finance_query_agent.query_builder import QueryBuilder
from finance_query_agent.schemas.mapping import (
    AmountConvention,
    ColumnRef,
    JoinDef,
    SchemaMapping,
    TableMapping,
)
from finance_query_agent.schemas.tool_results import Transaction
from finance_query_agent.tools import AgentDeps
from finance_query_agent.tools.transactions import search_transactions


def _make_schema() -> SchemaMapping:
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
                "balance": "balance",
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


def _make_deps(
    fetch_result: list | None = None,
    fetchrow_result: dict | None = None,
) -> AgentDeps:
    schema = _make_schema()
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=fetch_result or [])
    conn.fetchrow = AsyncMock(return_value=fetchrow_result)
    return AgentDeps(
        connection=conn,
        query_builder=QueryBuilder(schema),
        schema=schema,
        user_id="user-1",
    )


def _make_ctx(deps: AgentDeps) -> RunContext[AgentDeps]:
    ctx = MagicMock(spec=RunContext)
    ctx.deps = deps
    return ctx


class TestSearchTransactions:
    @pytest.mark.asyncio
    async def test_maps_rows(self):
        rows = [
            {
                "date": date(2024, 1, 15),
                "amount": Decimal("-50.00"),
                "description": "Netflix",
                "currency": "USD",
                "category": "Entertainment",
            },
        ]
        deps = _make_deps(fetch_result=rows, fetchrow_result={"total_count": 1})
        ctx = _make_ctx(deps)

        result = await search_transactions(ctx)

        assert len(result.transactions) == 1
        assert isinstance(result.transactions[0], Transaction)
        assert result.transactions[0].description == "Netflix"
        assert result.total_count == 1
        assert result.has_more is False

    @pytest.mark.asyncio
    async def test_has_more_when_more_results(self):
        rows = [
            {
                "date": date(2024, 1, i),
                "amount": Decimal("10"),
                "description": f"tx-{i}",
                "currency": "USD",
                "category": None,
            }
            for i in range(1, 21)
        ]
        deps = _make_deps(fetch_result=rows, fetchrow_result={"total_count": 50})
        ctx = _make_ctx(deps)

        result = await search_transactions(ctx, limit=20, offset=0)

        assert result.has_more is True
        assert result.total_count == 50

    @pytest.mark.asyncio
    async def test_records_tool_call(self):
        deps = _make_deps(fetchrow_result={"total_count": 0})
        ctx = _make_ctx(deps)

        await search_transactions(ctx, query="test", direction="expense")

        tc = deps.tool_calls[0]
        assert tc.tool_name == "search_transactions"
        assert tc.parameters["query"] == "test"
        assert tc.parameters["direction"] == "expense"

    @pytest.mark.asyncio
    async def test_null_count_row(self):
        deps = _make_deps(fetchrow_result=None)
        ctx = _make_ctx(deps)

        result = await search_transactions(ctx)
        assert result.total_count == 0
