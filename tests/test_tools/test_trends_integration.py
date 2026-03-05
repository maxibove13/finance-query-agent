"""Integration tests for trend tools — runs against seeded testcontainers Postgres."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from pydantic_ai import RunContext

from finance_query_agent.connection import Connection
from finance_query_agent.query_builder import QueryBuilder
from finance_query_agent.schemas.mapping import SchemaMapping
from finance_query_agent.schemas.tool_results import CategoryBreakdown, PeriodComparison, TrendPoint
from finance_query_agent.tools import AgentDeps
from finance_query_agent.tools.trends import compare_periods, get_category_breakdown, get_spending_trend

SEED_USER_1 = 1


def _make_ctx(conn: Connection, qb: QueryBuilder, schema: SchemaMapping, user_id: str) -> RunContext[AgentDeps]:
    deps = AgentDeps(connection=conn, query_builder=qb, schema=schema, user_id=user_id)
    ctx = MagicMock(spec=RunContext)
    ctx.deps = deps
    return ctx


class TestComparePeriodsIntegration:
    @pytest.mark.asyncio
    async def test_total_comparison(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await compare_periods(
            ctx,
            date(2025, 10, 1),
            date(2025, 10, 31),
            date(2025, 11, 1),
            date(2025, 11, 30),
        )

        usd = [r for r in result if r.currency == "USD"]
        assert len(usd) >= 1
        assert all(isinstance(r, PeriodComparison) for r in result)

        total = [r for r in usd if r.group_label == "Total"]
        assert len(total) == 1
        # Oct USD expenses: 150 + 85.50 + 45 + 12.99 = 293.49
        assert total[0].period_a_total == Decimal("293.49")
        assert total[0].absolute_change is not None
        assert total[0].percentage_change is not None

    @pytest.mark.asyncio
    async def test_category_comparison(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await compare_periods(
            ctx,
            date(2025, 10, 1),
            date(2025, 10, 31),
            date(2025, 11, 1),
            date(2025, 11, 30),
            group_by="category",
        )

        usd = [r for r in result if r.currency == "USD"]
        categories = {r.group_label for r in usd}
        assert "groceries" in categories

    @pytest.mark.asyncio
    async def test_merchant_comparison(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await compare_periods(
            ctx,
            date(2025, 10, 1),
            date(2025, 10, 31),
            date(2025, 11, 1),
            date(2025, 11, 30),
            group_by="merchant",
        )

        usd = [r for r in result if r.currency == "USD"]
        merchants = {r.group_label for r in usd}
        assert "Whole Foods" in merchants


class TestGetSpendingTrendIntegration:
    @pytest.mark.asyncio
    async def test_monthly_trend(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await get_spending_trend(ctx, date(2025, 10, 1), date(2026, 1, 31))

        usd = [r for r in result if r.currency == "USD"]
        assert len(usd) >= 3  # Oct, Nov, Dec, Jan
        assert all(isinstance(r, TrendPoint) for r in result)

        labels = [r.period_label for r in usd]
        assert "2025-10" in labels
        assert "2025-11" in labels

    @pytest.mark.asyncio
    async def test_weekly_trend(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await get_spending_trend(ctx, date(2025, 10, 1), date(2025, 10, 31), granularity="week")

        usd = [r for r in result if r.currency == "USD"]
        assert len(usd) > 0
        # Week labels should have W format
        for r in usd:
            assert "W" in r.period_label

    @pytest.mark.asyncio
    async def test_category_filter(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await get_spending_trend(ctx, date(2025, 10, 1), date(2026, 2, 28), category="groceries")

        usd = [r for r in result if r.currency == "USD"]
        assert len(usd) > 0


class TestGetCategoryBreakdownIntegration:
    @pytest.mark.asyncio
    async def test_returns_percentages(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await get_category_breakdown(ctx, date(2025, 10, 1), date(2025, 10, 31))

        usd = [r for r in result if r.currency == "USD"]
        assert len(usd) > 0
        assert all(isinstance(r, CategoryBreakdown) for r in result)

        # Percentages should sum to ~100 within each currency
        total_pct = sum(r.percentage for r in usd)
        assert total_pct == pytest.approx(100.0, abs=0.1)

    @pytest.mark.asyncio
    async def test_categories_present(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await get_category_breakdown(ctx, date(2025, 10, 1), date(2025, 10, 31))

        usd = [r for r in result if r.currency == "USD"]
        categories = {r.category for r in usd}
        assert "groceries" in categories
        assert "transport" in categories
        assert "entertainment" in categories
