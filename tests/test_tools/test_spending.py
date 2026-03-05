"""Tests for spending tools — mock connection, verify row mapping and ToolCallRecord."""

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
from finance_query_agent.schemas.tool_results import AccountSummary, CategorySpending, MonthlyTotal
from finance_query_agent.tools import AgentDeps
from finance_query_agent.tools.spending import (
    _prepare_balance_summary,
    get_balance_summary,
    get_monthly_totals,
    get_spending_by_category,
)


def _make_schema(*, with_balance: bool = True) -> SchemaMapping:
    cols = {
        "date": "issued_at",
        "amount": "amount",
        "description": "description",
        "user_id": ColumnRef(table="accounts", column="user_id"),
        "currency": ColumnRef(table="accounts", column="currency"),
        "account_id": "account_id",
    }
    if with_balance:
        cols["balance"] = "balance"

    return SchemaMapping(
        transactions=TableMapping(
            table="account_movements",
            columns=cols,
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


def _make_deps(schema: SchemaMapping | None = None, fetch_result: list | None = None) -> AgentDeps:
    schema = schema or _make_schema()
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=fetch_result or [])
    conn.fetchrow = AsyncMock(return_value=None)
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


class TestGetSpendingByCategory:
    @pytest.mark.asyncio
    async def test_maps_rows_correctly(self):
        rows = [
            {"category": "Food", "total_amount": Decimal("150.00"), "transaction_count": 5, "currency": "USD"},
            {"category": "Transport", "total_amount": Decimal("80.00"), "transaction_count": 3, "currency": "USD"},
        ]
        deps = _make_deps(fetch_result=rows)
        ctx = _make_ctx(deps)

        result = await get_spending_by_category(ctx, date(2024, 1, 1), date(2024, 1, 31))

        assert len(result) == 2
        assert isinstance(result[0], CategorySpending)
        assert result[0].category == "Food"
        assert result[0].total_amount == Decimal("150.00")
        assert result[1].transaction_count == 3

    @pytest.mark.asyncio
    async def test_records_tool_call(self):
        deps = _make_deps()
        ctx = _make_ctx(deps)

        await get_spending_by_category(ctx, date(2024, 1, 1), date(2024, 1, 31))

        assert len(deps.tool_calls) == 1
        tc = deps.tool_calls[0]
        assert tc.tool_name == "get_spending_by_category"
        assert tc.parameters["period_start"] == "2024-01-01"
        assert tc.row_count == 0

    @pytest.mark.asyncio
    async def test_empty_results(self):
        deps = _make_deps()
        ctx = _make_ctx(deps)

        result = await get_spending_by_category(ctx, date(2024, 1, 1), date(2024, 1, 31))
        assert result == []

    @pytest.mark.asyncio
    async def test_passes_categories_filter(self):
        deps = _make_deps()
        ctx = _make_ctx(deps)

        await get_spending_by_category(ctx, date(2024, 1, 1), date(2024, 1, 31), categories=["Food"])

        tc = deps.tool_calls[0]
        assert tc.parameters["categories"] == ["Food"]


class TestGetMonthlyTotals:
    @pytest.mark.asyncio
    async def test_maps_rows(self):
        rows = [
            {"year": 2024, "month": 1, "total_amount": Decimal("500"), "transaction_count": 10, "currency": "USD"},
        ]
        deps = _make_deps(fetch_result=rows)
        ctx = _make_ctx(deps)

        result = await get_monthly_totals(ctx, start_month=1, start_year=2024, end_month=6, end_year=2024)

        assert len(result) == 1
        assert isinstance(result[0], MonthlyTotal)
        assert result[0].year == 2024
        assert result[0].month == 1

    @pytest.mark.asyncio
    async def test_records_tool_call(self):
        deps = _make_deps()
        ctx = _make_ctx(deps)

        await get_monthly_totals(ctx, start_month=1, start_year=2024, end_month=6, end_year=2024, account_id="a1")

        tc = deps.tool_calls[0]
        assert tc.tool_name == "get_monthly_totals"
        assert tc.parameters["start_month"] == 1
        assert tc.parameters["start_year"] == 2024
        assert tc.parameters["end_month"] == 6
        assert tc.parameters["end_year"] == 2024
        assert tc.parameters["account_id"] == "a1"

    @pytest.mark.asyncio
    async def test_single_month(self):
        """Single month range: start == end computes correct date boundaries."""
        rows = [
            {"year": 2026, "month": 2, "total_amount": Decimal("200"), "transaction_count": 4, "currency": "USD"},
        ]
        deps = _make_deps(fetch_result=rows)
        ctx = _make_ctx(deps)

        result = await get_monthly_totals(ctx, start_month=2, start_year=2026, end_month=2, end_year=2026)

        assert len(result) == 1
        # Feb 2026 is not a leap year — verify the tool handles it (no invalid date error)
        tc = deps.tool_calls[0]
        assert tc.parameters["start_month"] == 2
        assert tc.parameters["end_month"] == 2
        # Verify the query received correct date boundaries
        call_args = deps.connection.fetch.call_args
        params = call_args[0][1:]  # skip SQL string
        # period_start should be 2026-02-01, period_end should be 2026-02-28
        assert date(2026, 2, 1) in params
        assert date(2026, 2, 28) in params

    @pytest.mark.asyncio
    async def test_cross_year_range(self):
        """Cross-year range: Nov 2025 to Feb 2026."""
        deps = _make_deps()
        ctx = _make_ctx(deps)

        await get_monthly_totals(ctx, start_month=11, start_year=2025, end_month=2, end_year=2026)

        tc = deps.tool_calls[0]
        assert tc.parameters["start_year"] == 2025
        assert tc.parameters["end_year"] == 2026
        call_args = deps.connection.fetch.call_args
        params = call_args[0][1:]
        assert date(2025, 11, 1) in params
        assert date(2026, 2, 28) in params


class TestGetBalanceSummary:
    @pytest.mark.asyncio
    async def test_maps_rows(self):
        rows = [
            {
                "account_name": "Checking",
                "latest_balance": Decimal("1000"),
                "last_transaction_date": date(2024, 1, 15),
                "currency": "USD",
            },
        ]
        deps = _make_deps(fetch_result=rows)
        ctx = _make_ctx(deps)

        result = await get_balance_summary(ctx)

        assert len(result) == 1
        assert isinstance(result[0], AccountSummary)
        assert result[0].account_name == "Checking"
        assert result[0].latest_balance == Decimal("1000")

    @pytest.mark.asyncio
    async def test_records_tool_call(self):
        deps = _make_deps()
        ctx = _make_ctx(deps)

        await get_balance_summary(ctx, account_id="a1")

        tc = deps.tool_calls[0]
        assert tc.tool_name == "get_balance_summary"
        assert tc.parameters["account_id"] == "a1"


class TestPrepareBalanceSummary:
    @pytest.mark.asyncio
    async def test_returns_tool_when_balance_mapped(self):
        schema = _make_schema(with_balance=True)
        deps = _make_deps(schema=schema)
        ctx = _make_ctx(deps)
        tool_def = MagicMock()

        result = await _prepare_balance_summary(ctx, tool_def)
        assert result is tool_def

    @pytest.mark.asyncio
    async def test_returns_none_when_no_balance(self):
        schema = _make_schema(with_balance=False)
        deps = _make_deps(schema=schema)
        ctx = _make_ctx(deps)
        tool_def = MagicMock()

        result = await _prepare_balance_summary(ctx, tool_def)
        assert result is None
