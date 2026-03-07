"""Tests for tool-level error logging on fetch failure."""

from __future__ import annotations

import logging
from datetime import date
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
    ViewMapping,
)
from finance_query_agent.tools import AgentDeps
from finance_query_agent.tools.recurring import get_recurring_expenses
from finance_query_agent.tools.transactions import search_transactions
from finance_query_agent.tools.unified import query_balance_history, query_expenses, query_income


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
        categories=TableMapping(table="tags", columns={"id": "id", "name": "name"}, user_scoped=False),
        accounts=TableMapping(table="accounts", columns={"id": "id", "name": "alias", "user_id": "user_id"}),
        unified_expenses=ViewMapping(
            table="expenses_mv",
            columns={
                "user_id": "user_id",
                "date": "filter_at",
                "usd_amount": "usd_amount",
                "local_amount": "local_amount",
                "category": "category",
                "merchant": "description",
            },
        ),
        unified_income=ViewMapping(
            table="income_mv",
            columns={
                "user_id": "user_id",
                "month": "month",
                "usd_amount": "usd_amount",
                "local_amount": "local_amount",
            },
        ),
        unified_balances=ViewMapping(
            table="balances_mv",
            columns={
                "user_id": "user_id",
                "date": "snapshot_date",
                "usd_total": "usd_total",
                "local_total": "local_total",
            },
        ),
    )


def _make_failing_deps() -> AgentDeps:
    schema = _make_schema()
    conn = AsyncMock()
    conn.fetch = AsyncMock(side_effect=RuntimeError("connection lost"))
    conn.fetchrow = AsyncMock(side_effect=RuntimeError("connection lost"))
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


_START = date(2024, 1, 1)
_END = date(2024, 12, 31)


class TestToolErrorLogging:
    """Each predefined tool should logger.error before re-raising on fetch failure."""

    @pytest.mark.asyncio
    async def test_search_transactions_logs_error(self, caplog):
        deps = _make_failing_deps()
        ctx = _make_ctx(deps)
        with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError):
            await search_transactions(ctx)
        assert any("search_transactions" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_get_recurring_expenses_logs_error(self, caplog):
        deps = _make_failing_deps()
        ctx = _make_ctx(deps)
        with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError):
            await get_recurring_expenses(ctx, _START, _END)
        assert any("get_recurring_expenses" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_query_expenses_logs_error(self, caplog):
        deps = _make_failing_deps()
        ctx = _make_ctx(deps)
        with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError):
            await query_expenses(ctx, _START, _END)
        assert any("query_expenses" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_query_income_logs_error(self, caplog):
        deps = _make_failing_deps()
        ctx = _make_ctx(deps)
        with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError):
            await query_income(ctx, _START, _END)
        assert any("query_income" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_query_balance_history_logs_error(self, caplog):
        deps = _make_failing_deps()
        ctx = _make_ctx(deps)
        with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError):
            await query_balance_history(ctx)
        assert any("query_balance_history" in r.message for r in caplog.records)
