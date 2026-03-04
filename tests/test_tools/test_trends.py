"""Tests for trend tools — mock connection, verify row mapping and computed fields."""

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
from finance_query_agent.schemas.tool_results import CategoryBreakdown, PeriodComparison, TrendPoint
from finance_query_agent.tools import AgentDeps
from finance_query_agent.tools.trends import compare_periods, get_category_breakdown, get_spending_trend


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


def _make_deps(fetch_result: list | None = None) -> AgentDeps:
    schema = _make_schema()
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=fetch_result or [])
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


class TestComparePeriods:
    @pytest.mark.asyncio
    async def test_computes_change(self):
        rows = [
            {"group_label": "Total", "currency": "USD", "period_a_total": 100, "period_b_total": 150},
        ]
        deps = _make_deps(fetch_result=rows)
        ctx = _make_ctx(deps)

        result = await compare_periods(ctx, date(2024, 1, 1), date(2024, 1, 31), date(2024, 2, 1), date(2024, 2, 28))

        assert len(result) == 1
        assert isinstance(result[0], PeriodComparison)
        assert result[0].absolute_change == Decimal("50")
        assert result[0].percentage_change == pytest.approx(50.0)

    @pytest.mark.asyncio
    async def test_percentage_none_when_zero_base(self):
        rows = [
            {"group_label": "Total", "currency": "USD", "period_a_total": 0, "period_b_total": 100},
        ]
        deps = _make_deps(fetch_result=rows)
        ctx = _make_ctx(deps)

        result = await compare_periods(ctx, date(2024, 1, 1), date(2024, 1, 31), date(2024, 2, 1), date(2024, 2, 28))

        assert result[0].percentage_change is None
        assert result[0].absolute_change == Decimal("100")

    @pytest.mark.asyncio
    async def test_negative_change(self):
        rows = [
            {"group_label": "Food", "currency": "USD", "period_a_total": 200, "period_b_total": 150},
        ]
        deps = _make_deps(fetch_result=rows)
        ctx = _make_ctx(deps)

        result = await compare_periods(
            ctx,
            date(2024, 1, 1),
            date(2024, 1, 31),
            date(2024, 2, 1),
            date(2024, 2, 28),
            group_by="category",
        )

        assert result[0].absolute_change == Decimal("-50")
        assert result[0].percentage_change == pytest.approx(-25.0)

    @pytest.mark.asyncio
    async def test_records_tool_call(self):
        deps = _make_deps()
        ctx = _make_ctx(deps)

        await compare_periods(ctx, date(2024, 1, 1), date(2024, 1, 31), date(2024, 2, 1), date(2024, 2, 28))

        tc = deps.tool_calls[0]
        assert tc.tool_name == "compare_periods"
        assert tc.parameters["group_by"] == "total"


class TestGetSpendingTrend:
    @pytest.mark.asyncio
    async def test_maps_rows(self):
        rows = [
            {"period_label": "2024-01", "total_amount": Decimal("500"), "transaction_count": 10, "currency": "USD"},
            {"period_label": "2024-02", "total_amount": Decimal("600"), "transaction_count": 12, "currency": "USD"},
        ]
        deps = _make_deps(fetch_result=rows)
        ctx = _make_ctx(deps)

        result = await get_spending_trend(ctx, date(2024, 1, 1), date(2024, 12, 31))

        assert len(result) == 2
        assert isinstance(result[0], TrendPoint)
        assert result[0].period_label == "2024-01"

    @pytest.mark.asyncio
    async def test_records_tool_call(self):
        deps = _make_deps()
        ctx = _make_ctx(deps)

        await get_spending_trend(ctx, date(2024, 1, 1), date(2024, 12, 31), granularity="week", category="Food")

        tc = deps.tool_calls[0]
        assert tc.tool_name == "get_spending_trend"
        assert tc.parameters["granularity"] == "week"
        assert tc.parameters["category"] == "Food"


class TestGetCategoryBreakdown:
    @pytest.mark.asyncio
    async def test_maps_rows(self):
        rows = [
            {"category": "Food", "total_amount": Decimal("300"), "percentage": Decimal("60.00"), "currency": "USD"},
            {
                "category": "Transport",
                "total_amount": Decimal("200"),
                "percentage": Decimal("40.00"),
                "currency": "USD",
            },
        ]
        deps = _make_deps(fetch_result=rows)
        ctx = _make_ctx(deps)

        result = await get_category_breakdown(ctx, date(2024, 1, 1), date(2024, 12, 31))

        assert len(result) == 2
        assert isinstance(result[0], CategoryBreakdown)
        assert result[0].percentage == pytest.approx(60.0)

    @pytest.mark.asyncio
    async def test_records_tool_call(self):
        deps = _make_deps()
        ctx = _make_ctx(deps)

        await get_category_breakdown(ctx, date(2024, 1, 1), date(2024, 12, 31), account_id="a1")

        tc = deps.tool_calls[0]
        assert tc.tool_name == "get_category_breakdown"
        assert tc.parameters["account_id"] == "a1"
