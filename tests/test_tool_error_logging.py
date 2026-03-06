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
)
from finance_query_agent.tools import AgentDeps
from finance_query_agent.tools.recurring import get_recurring_expenses
from finance_query_agent.tools.spending import (
    get_balance_summary,
    get_monthly_totals,
    get_spending_by_category,
)
from finance_query_agent.tools.transactions import get_top_merchants, search_transactions
from finance_query_agent.tools.trends import (
    compare_periods,
    get_category_breakdown,
    get_spending_trend,
)


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
    async def test_get_spending_by_category_logs_error(self, caplog):
        deps = _make_failing_deps()
        ctx = _make_ctx(deps)
        with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError):
            await get_spending_by_category(ctx, _START, _END)
        assert any("get_spending_by_category" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_get_monthly_totals_logs_error(self, caplog):
        deps = _make_failing_deps()
        ctx = _make_ctx(deps)
        with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError):
            await get_monthly_totals(ctx, start_month=1, start_year=2024, end_month=12, end_year=2024)
        assert any("get_monthly_totals" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_get_balance_summary_logs_error(self, caplog):
        deps = _make_failing_deps()
        ctx = _make_ctx(deps)
        with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError):
            await get_balance_summary(ctx)
        assert any("get_balance_summary" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_search_transactions_logs_error(self, caplog):
        deps = _make_failing_deps()
        ctx = _make_ctx(deps)
        with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError):
            await search_transactions(ctx)
        assert any("search_transactions" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_get_top_merchants_logs_error(self, caplog):
        deps = _make_failing_deps()
        ctx = _make_ctx(deps)
        with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError):
            await get_top_merchants(ctx, _START, _END)
        assert any("get_top_merchants" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_compare_periods_logs_error(self, caplog):
        deps = _make_failing_deps()
        ctx = _make_ctx(deps)
        with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError):
            await compare_periods(ctx, _START, date(2024, 6, 30), date(2024, 7, 1), _END)
        assert any("compare_periods" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_get_spending_trend_logs_error(self, caplog):
        deps = _make_failing_deps()
        ctx = _make_ctx(deps)
        with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError):
            await get_spending_trend(ctx, _START, _END)
        assert any("get_spending_trend" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_get_category_breakdown_logs_error(self, caplog):
        deps = _make_failing_deps()
        ctx = _make_ctx(deps)
        with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError):
            await get_category_breakdown(ctx, _START, _END)
        assert any("get_category_breakdown" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_get_recurring_expenses_logs_error(self, caplog):
        deps = _make_failing_deps()
        ctx = _make_ctx(deps)
        with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError):
            await get_recurring_expenses(ctx, _START, _END)
        assert any("get_recurring_expenses" in r.message for r in caplog.records)
