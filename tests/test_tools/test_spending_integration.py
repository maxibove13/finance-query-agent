"""Integration tests for spending tools — runs against seeded testcontainers Postgres."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from pydantic_ai import RunContext

from finance_query_agent.connection import Connection
from finance_query_agent.query_builder import QueryBuilder
from finance_query_agent.schemas.mapping import SchemaMapping
from finance_query_agent.schemas.tool_results import AccountSummary, CategorySpending, MonthlyTotal
from finance_query_agent.tools import AgentDeps
from finance_query_agent.tools.spending import get_balance_summary, get_monthly_totals, get_spending_by_category

SEED_USER_1 = 1
SEED_USER_2 = 2


def _make_ctx(conn: Connection, qb: QueryBuilder, schema: SchemaMapping, user_id: str) -> RunContext[AgentDeps]:
    deps = AgentDeps(connection=conn, query_builder=qb, schema=schema, user_id=user_id)
    ctx = MagicMock(spec=RunContext)
    ctx.deps = deps
    return ctx


class TestGetSpendingByCategoryIntegration:
    @pytest.mark.asyncio
    async def test_returns_category_totals(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await get_spending_by_category(ctx, date(2025, 10, 1), date(2025, 10, 31))

        assert len(result) > 0
        assert all(isinstance(r, CategorySpending) for r in result)
        # Groceries should be in there (Whole Foods + Trader Joes in Oct)
        grocery = [r for r in result if r.category == "groceries" and r.currency == "USD"]
        assert len(grocery) == 1
        assert grocery[0].total_amount == Decimal("235.50")  # 150 + 85.50
        assert grocery[0].transaction_count == 2

    @pytest.mark.asyncio
    async def test_filters_by_category(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await get_spending_by_category(ctx, date(2025, 10, 1), date(2025, 10, 31), categories=["transport"])

        assert len(result) == 1
        assert result[0].category == "transport"
        assert result[0].total_amount == Decimal("45.00")

    @pytest.mark.asyncio
    async def test_user_isolation(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_2)

        result = await get_spending_by_category(ctx, date(2025, 11, 1), date(2025, 11, 30))

        # User 2 only has one grocery transaction
        assert len(result) == 1
        assert result[0].total_amount == Decimal("200.00")

    @pytest.mark.asyncio
    async def test_multi_currency(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        # Nov spans both USD and UYU accounts
        result = await get_spending_by_category(ctx, date(2025, 11, 1), date(2025, 11, 30))

        currencies = {r.currency for r in result}
        assert "USD" in currencies
        assert "UYU" in currencies

    @pytest.mark.asyncio
    async def test_excludes_income(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await get_spending_by_category(ctx, date(2025, 10, 1), date(2025, 10, 31))

        # "Salary Oct" is credit, should not appear
        all_categories = [r.category for r in result]
        assert "Uncategorized" not in all_categories  # Salary is uncategorized + credit


class TestGetMonthlyTotalsIntegration:
    @pytest.mark.asyncio
    async def test_returns_monthly_data(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await get_monthly_totals(ctx, date(2025, 10, 1), date(2025, 12, 31))

        assert len(result) > 0
        assert all(isinstance(r, MonthlyTotal) for r in result)

        # Check Oct USD totals: 150 + 85.50 + 45 + 12.99 = 293.49
        oct_usd = [r for r in result if r.year == 2025 and r.month == 10 and r.currency == "USD"]
        assert len(oct_usd) == 1
        assert oct_usd[0].total_amount == Decimal("293.49")

    @pytest.mark.asyncio
    async def test_includes_multiple_months(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await get_monthly_totals(ctx, date(2025, 10, 1), date(2026, 2, 28))

        usd_months = {(r.year, r.month) for r in result if r.currency == "USD"}
        assert (2025, 10) in usd_months
        assert (2025, 11) in usd_months
        assert (2025, 12) in usd_months


class TestGetBalanceSummaryIntegration:
    @pytest.mark.asyncio
    async def test_returns_latest_balance(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await get_balance_summary(ctx)

        assert len(result) > 0
        assert all(isinstance(r, AccountSummary) for r in result)

        # acc-1 latest is am-24 (Feb 25, balance 18892.03)
        checking = [r for r in result if r.account_name == "Checking"]
        assert len(checking) == 1
        assert checking[0].latest_balance == Decimal("18892.03")

    @pytest.mark.asyncio
    async def test_filter_by_account(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await get_balance_summary(ctx, account_id=2)

        assert len(result) == 1
        assert result[0].currency == "UYU"
