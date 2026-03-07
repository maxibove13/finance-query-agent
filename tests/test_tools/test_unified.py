"""Tests for unified view tools — mock connection, verify SQL generation and row mapping."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic_ai import RunContext
from pydantic_ai.tools import ToolDefinition

from finance_query_agent.query_builder import QueryBuilder
from finance_query_agent.schemas.mapping import (
    AmountConvention,
    ColumnRef,
    JoinDef,
    SchemaMapping,
    TableMapping,
    ViewMapping,
)
from finance_query_agent.schemas.unified_results import BalanceSnapshot, ExpenseGroup, IncomeMonth
from finance_query_agent.tools import AgentDeps
from finance_query_agent.tools.unified import (
    _prepare_query_balance_history,
    _prepare_query_expenses,
    _prepare_query_income,
    query_balance_history,
    query_expenses,
    query_income,
)


def _base_schema_kwargs() -> dict:
    """Base table mappings reused across tests."""
    return {
        "transactions": TableMapping(
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
        "categories": TableMapping(
            table="tags",
            columns={"id": "id", "name": "name"},
            user_scoped=False,
        ),
        "accounts": TableMapping(
            table="accounts",
            columns={"id": "id", "name": "alias", "user_id": "user_id"},
        ),
    }


def _make_schema(
    *,
    with_expenses: bool = False,
    with_income: bool = False,
    with_balances: bool = False,
    with_breakdown: bool = False,
) -> SchemaMapping:
    kwargs = _base_schema_kwargs()
    if with_expenses:
        kwargs["unified_expenses"] = ViewMapping(
            table="historical_expenses_mv",
            columns={
                "user_id": "user_id",
                "date": "filter_at",
                "usd_amount": "usd_amount",
                "local_amount": "local_amount",
                "category": "category",
                "merchant": "description",
            },
        )
    if with_income:
        kwargs["unified_income"] = ViewMapping(
            table="historical_incomes_mv",
            columns={
                "user_id": "user_id",
                "month": "month",
                "usd_amount": "usd_amount",
                "local_amount": "local_amount",
            },
        )
    if with_balances:
        cols = {
            "user_id": "user_id",
            "date": "snapshot_date",
            "usd_total": "usd_total",
            "local_total": "local_total",
        }
        if with_breakdown:
            cols["currency_breakdown"] = "currency_breakdown"
        kwargs["unified_balances"] = ViewMapping(
            table="historical_balances_mv",
            columns=cols,
        )
    return SchemaMapping(**kwargs)


def _make_deps(schema: SchemaMapping, fetch_result: list | None = None) -> AgentDeps:
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=fetch_result or [])
    return AgentDeps(
        connection=conn,
        query_builder=QueryBuilder(_make_schema()),  # not used by unified tools
        schema=schema,
        user_id="user-1",
    )


def _make_ctx(deps: AgentDeps) -> RunContext[AgentDeps]:
    ctx = MagicMock(spec=RunContext)
    ctx.deps = deps
    return ctx


# -----------------------------------------------------------------------
# ViewMapping validation
# -----------------------------------------------------------------------


class TestViewMappingValidation:
    def test_missing_required_expense_key(self):
        kwargs = _base_schema_kwargs()
        kwargs["unified_expenses"] = ViewMapping(
            table="expenses_mv",
            columns={"user_id": "user_id", "date": "d"},  # missing usd_amount, etc.
        )
        with pytest.raises(ValueError, match="unified_expenses missing required column mappings"):
            SchemaMapping(**kwargs)

    def test_missing_required_income_key(self):
        kwargs = _base_schema_kwargs()
        kwargs["unified_income"] = ViewMapping(
            table="income_mv",
            columns={"user_id": "user_id"},  # missing month, usd_amount, local_amount
        )
        with pytest.raises(ValueError, match="unified_income missing required column mappings"):
            SchemaMapping(**kwargs)

    def test_missing_required_balances_key(self):
        kwargs = _base_schema_kwargs()
        kwargs["unified_balances"] = ViewMapping(
            table="bal_mv",
            columns={"user_id": "uid", "date": "d"},  # missing usd_total, local_total
        )
        with pytest.raises(ValueError, match="unified_balances missing required column mappings"):
            SchemaMapping(**kwargs)

    def test_valid_expense_view(self):
        schema = _make_schema(with_expenses=True)
        assert schema.unified_expenses is not None
        assert schema.unified_expenses.table == "historical_expenses_mv"


# -----------------------------------------------------------------------
# Prepare callbacks
# -----------------------------------------------------------------------


class TestPrepareCallbacks:
    @pytest.mark.asyncio
    async def test_expenses_visible_when_configured(self):
        schema = _make_schema(with_expenses=True)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)
        tool_def = MagicMock(spec=ToolDefinition)

        result = await _prepare_query_expenses(ctx, tool_def)
        assert result is tool_def

    @pytest.mark.asyncio
    async def test_expenses_hidden_when_not_configured(self):
        schema = _make_schema(with_expenses=False)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)
        tool_def = MagicMock(spec=ToolDefinition)

        result = await _prepare_query_expenses(ctx, tool_def)
        assert result is None

    @pytest.mark.asyncio
    async def test_income_visible_when_configured(self):
        schema = _make_schema(with_income=True)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)
        tool_def = MagicMock(spec=ToolDefinition)

        result = await _prepare_query_income(ctx, tool_def)
        assert result is tool_def

    @pytest.mark.asyncio
    async def test_income_hidden_when_not_configured(self):
        schema = _make_schema(with_income=False)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)
        tool_def = MagicMock(spec=ToolDefinition)

        result = await _prepare_query_income(ctx, tool_def)
        assert result is None

    @pytest.mark.asyncio
    async def test_balances_visible_when_configured(self):
        schema = _make_schema(with_balances=True)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)
        tool_def = MagicMock(spec=ToolDefinition)

        result = await _prepare_query_balance_history(ctx, tool_def)
        assert result is tool_def

    @pytest.mark.asyncio
    async def test_balances_hidden_when_not_configured(self):
        schema = _make_schema(with_balances=False)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)
        tool_def = MagicMock(spec=ToolDefinition)

        result = await _prepare_query_balance_history(ctx, tool_def)
        assert result is None


# -----------------------------------------------------------------------
# query_expenses
# -----------------------------------------------------------------------


class TestQueryExpenses:
    @pytest.mark.asyncio
    async def test_total_grouping_usd(self):
        rows = [{"label": "Total", "total_amount": Decimal("1500.00"), "transaction_count": 42}]
        schema = _make_schema(with_expenses=True)
        deps = _make_deps(schema, fetch_result=rows)
        ctx = _make_ctx(deps)

        result = await query_expenses(ctx, date(2025, 1, 1), date(2025, 1, 31))

        assert len(result) == 1
        assert isinstance(result[0], ExpenseGroup)
        assert result[0].label == "Total"
        assert result[0].total_amount == Decimal("1500.00")
        assert result[0].transaction_count == 42
        assert result[0].currency == "usd"

        # Verify SQL uses usd_amount
        sql = deps.connection.fetch.call_args[0][0]
        assert "usd_amount" in sql
        assert "user-1" == deps.connection.fetch.call_args[0][1]

    @pytest.mark.asyncio
    async def test_category_grouping_local(self):
        rows = [
            {"label": "Food", "total_amount": Decimal("800"), "transaction_count": 20},
            {"label": "Transport", "total_amount": Decimal("300"), "transaction_count": 10},
        ]
        schema = _make_schema(with_expenses=True)
        deps = _make_deps(schema, fetch_result=rows)
        ctx = _make_ctx(deps)

        result = await query_expenses(ctx, date(2025, 1, 1), date(2025, 1, 31), group_by="category", currency="local")

        assert len(result) == 2
        assert result[0].label == "Food"
        assert result[0].currency == "local"
        sql = deps.connection.fetch.call_args[0][0]
        assert "local_amount" in sql
        assert "GROUP BY" in sql

    @pytest.mark.asyncio
    async def test_month_grouping(self):
        rows = [{"label": "2025-01", "total_amount": Decimal("1000"), "transaction_count": 15}]
        schema = _make_schema(with_expenses=True)
        deps = _make_deps(schema, fetch_result=rows)
        ctx = _make_ctx(deps)

        result = await query_expenses(ctx, date(2025, 1, 1), date(2025, 3, 31), group_by="month")

        sql = deps.connection.fetch.call_args[0][0]
        assert "TO_CHAR" in sql
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_category_filter(self):
        schema = _make_schema(with_expenses=True)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)

        await query_expenses(ctx, date(2025, 1, 1), date(2025, 1, 31), category="Food")

        sql = deps.connection.fetch.call_args[0][0]
        params = deps.connection.fetch.call_args[0][1:]
        assert "category = $4" in sql
        assert "Food" in params

    @pytest.mark.asyncio
    async def test_merchant_filter_ilike(self):
        schema = _make_schema(with_expenses=True)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)

        await query_expenses(ctx, date(2025, 1, 1), date(2025, 1, 31), merchant="starbucks")

        sql = deps.connection.fetch.call_args[0][0]
        params = deps.connection.fetch.call_args[0][1:]
        assert "ILIKE" in sql
        assert "%starbucks%" in params

    @pytest.mark.asyncio
    async def test_limit(self):
        schema = _make_schema(with_expenses=True)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)

        await query_expenses(ctx, date(2025, 1, 1), date(2025, 1, 31), group_by="category", limit=5)

        sql = deps.connection.fetch.call_args[0][0]
        assert "LIMIT 5" in sql

    @pytest.mark.asyncio
    async def test_records_tool_call(self):
        schema = _make_schema(with_expenses=True)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)

        await query_expenses(ctx, date(2025, 1, 1), date(2025, 1, 31), group_by="merchant", currency="local")

        assert len(deps.tool_calls) == 1
        tc = deps.tool_calls[0]
        assert tc.tool_name == "query_expenses"
        assert tc.parameters["group_by"] == "merchant"
        assert tc.parameters["currency"] == "local"

    @pytest.mark.asyncio
    async def test_empty_results(self):
        schema = _make_schema(with_expenses=True)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)

        result = await query_expenses(ctx, date(2025, 1, 1), date(2025, 1, 31))
        assert result == []

    @pytest.mark.asyncio
    async def test_category_coalesce_in_sql(self):
        """Verify COALESCE wraps category and merchant group expressions to handle NULLs."""
        schema = _make_schema(with_expenses=True)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)

        await query_expenses(ctx, date(2025, 1, 1), date(2025, 1, 31), group_by="category")
        sql = deps.connection.fetch.call_args[0][0]
        assert "COALESCE(category, 'Uncategorized')" in sql

    @pytest.mark.asyncio
    async def test_merchant_coalesce_in_sql(self):
        schema = _make_schema(with_expenses=True)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)

        await query_expenses(ctx, date(2025, 1, 1), date(2025, 1, 31), group_by="merchant")
        sql = deps.connection.fetch.call_args[0][0]
        assert "COALESCE(description, 'Unknown')" in sql

    @pytest.mark.asyncio
    async def test_uses_mapped_column_names(self):
        """Verify the SQL uses column names from ViewMapping, not hardcoded MPI names."""
        schema = _make_schema(with_expenses=True)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)

        await query_expenses(ctx, date(2025, 1, 1), date(2025, 1, 31))

        sql = deps.connection.fetch.call_args[0][0]
        # The ViewMapping maps date -> filter_at, merchant -> description
        assert "filter_at" in sql
        assert "historical_expenses_mv" in sql


# -----------------------------------------------------------------------
# query_income
# -----------------------------------------------------------------------


class TestQueryIncome:
    @pytest.mark.asyncio
    async def test_maps_rows_usd(self):
        rows = [
            {"month_label": "2025/01", "total_amount": Decimal("3000.00")},
            {"month_label": "2025/02", "total_amount": Decimal("3200.00")},
        ]
        schema = _make_schema(with_income=True)
        deps = _make_deps(schema, fetch_result=rows)
        ctx = _make_ctx(deps)

        result = await query_income(ctx, period_start=date(2025, 1, 1), period_end=date(2025, 2, 28))

        assert len(result) == 2
        assert isinstance(result[0], IncomeMonth)
        assert result[0].month_label == "2025/01"
        assert result[0].total_amount == Decimal("3000.00")
        assert result[0].currency == "usd"

        sql = deps.connection.fetch.call_args[0][0]
        assert "usd_amount" in sql

    @pytest.mark.asyncio
    async def test_local_currency(self):
        schema = _make_schema(with_income=True)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)

        await query_income(ctx, period_start=date(2025, 1, 1), period_end=date(2025, 3, 31), currency="local")

        sql = deps.connection.fetch.call_args[0][0]
        assert "local_amount" in sql

    @pytest.mark.asyncio
    async def test_date_to_month_text_conversion(self):
        """Verify period_start/period_end dates are converted to 'YYYY/MM' text for the query."""
        schema = _make_schema(with_income=True)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)

        await query_income(ctx, period_start=date(2025, 3, 15), period_end=date(2025, 11, 20))

        params = deps.connection.fetch.call_args[0][1:]
        assert "2025/03" in params
        assert "2025/11" in params

    @pytest.mark.asyncio
    async def test_cross_year(self):
        schema = _make_schema(with_income=True)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)

        await query_income(ctx, period_start=date(2024, 11, 1), period_end=date(2025, 2, 28))

        params = deps.connection.fetch.call_args[0][1:]
        assert "2024/11" in params
        assert "2025/02" in params

    @pytest.mark.asyncio
    async def test_records_tool_call(self):
        schema = _make_schema(with_income=True)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)

        await query_income(ctx, period_start=date(2025, 1, 1), period_end=date(2025, 6, 30))

        tc = deps.tool_calls[0]
        assert tc.tool_name == "query_income"
        assert tc.parameters["period_start"] == "2025-01-01"
        assert tc.parameters["period_end"] == "2025-06-30"


# -----------------------------------------------------------------------
# query_balance_history
# -----------------------------------------------------------------------


class TestQueryBalanceHistory:
    @pytest.mark.asyncio
    async def test_latest_snapshot_no_dates(self):
        rows = [{"date": date(2025, 3, 5), "total_balance": Decimal("15000.00")}]
        schema = _make_schema(with_balances=True)
        deps = _make_deps(schema, fetch_result=rows)
        ctx = _make_ctx(deps)

        result = await query_balance_history(ctx)

        assert len(result) == 1
        assert isinstance(result[0], BalanceSnapshot)
        assert result[0].total_balance == Decimal("15000.00")

        sql = deps.connection.fetch.call_args[0][0]
        assert "LIMIT 1" in sql
        assert "ORDER BY" in sql

    @pytest.mark.asyncio
    async def test_local_currency(self):
        rows = [{"date": date(2025, 3, 5), "total_balance": Decimal("500000.00")}]
        schema = _make_schema(with_balances=True)
        deps = _make_deps(schema, fetch_result=rows)
        ctx = _make_ctx(deps)

        result = await query_balance_history(ctx, currency="local")

        sql = deps.connection.fetch.call_args[0][0]
        assert "local_total" in sql
        assert result[0].total_balance == Decimal("500000.00")

    @pytest.mark.asyncio
    async def test_monthly_granularity_with_dates(self):
        schema = _make_schema(with_balances=True)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)

        await query_balance_history(
            ctx, period_start=date(2025, 1, 1), period_end=date(2025, 3, 31), granularity="monthly"
        )

        sql = deps.connection.fetch.call_args[0][0]
        assert "DISTINCT ON" in sql
        assert "DATE_TRUNC('month'" in sql

    @pytest.mark.asyncio
    async def test_daily_granularity(self):
        schema = _make_schema(with_balances=True)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)

        await query_balance_history(
            ctx, period_start=date(2025, 3, 1), period_end=date(2025, 3, 5), granularity="daily"
        )

        sql = deps.connection.fetch.call_args[0][0]
        assert "DISTINCT ON" not in sql
        assert "ORDER BY" in sql

    @pytest.mark.asyncio
    async def test_include_breakdown(self):
        """include_breakdown=True returns JSONB currency_balances when column is mapped."""
        rows = [
            {
                "date": date(2025, 3, 5),
                "total_balance": Decimal("15000.00"),
                "currency_balances": '{"USD": 10000, "UYU": 250000}',
            }
        ]
        schema = _make_schema(with_balances=True, with_breakdown=True)
        deps = _make_deps(schema, fetch_result=rows)
        # AsyncMock doesn't track .keys() on dict rows, use real dicts
        deps.connection.fetch = AsyncMock(return_value=rows)
        ctx = _make_ctx(deps)

        result = await query_balance_history(ctx, include_breakdown=True)

        assert result[0].currency_balances is not None
        assert result[0].currency_balances["USD"] == Decimal("10000")
        assert result[0].currency_balances["UYU"] == Decimal("250000")

        sql = deps.connection.fetch.call_args[0][0]
        assert "currency_breakdown" in sql

    @pytest.mark.asyncio
    async def test_breakdown_not_mapped_no_breakdown(self):
        """When currency_breakdown column is not mapped, include_breakdown has no effect."""
        rows = [{"date": date(2025, 3, 5), "total_balance": Decimal("15000.00")}]
        schema = _make_schema(with_balances=True, with_breakdown=False)
        deps = _make_deps(schema, fetch_result=rows)
        ctx = _make_ctx(deps)

        result = await query_balance_history(ctx, include_breakdown=True)

        # Should still work, just no currency_balances
        assert result[0].total_balance == Decimal("15000.00")
        assert result[0].currency_balances is None
        sql = deps.connection.fetch.call_args[0][0]
        assert "currency_breakdown" not in sql

    @pytest.mark.asyncio
    async def test_records_tool_call(self):
        schema = _make_schema(with_balances=True)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)

        await query_balance_history(ctx, period_start=date(2025, 1, 1))

        tc = deps.tool_calls[0]
        assert tc.tool_name == "query_balance_history"
        assert tc.parameters["period_start"] == "2025-01-01"
        assert tc.parameters["granularity"] == "monthly"
        assert tc.parameters["include_breakdown"] is False

    @pytest.mark.asyncio
    async def test_uses_mapped_column_names(self):
        schema = _make_schema(with_balances=True)
        deps = _make_deps(schema)
        ctx = _make_ctx(deps)

        await query_balance_history(ctx)

        sql = deps.connection.fetch.call_args[0][0]
        # ViewMapping maps date -> snapshot_date
        assert "snapshot_date" in sql
        assert "historical_balances_mv" in sql
