"""Tests for recurring expense tool — post-processing logic is the key part."""

from __future__ import annotations

from datetime import date, timedelta
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
from finance_query_agent.schemas.tool_results import RecurringExpense
from finance_query_agent.tools import AgentDeps
from finance_query_agent.tools.recurring import (
    _classify_frequency,
    _coefficient_of_variation,
    _process_recurring_rows,
    get_recurring_expenses,
)

# ── Unit tests for post-processing helpers ────────────────────────


class TestClassifyFrequency:
    def test_weekly(self):
        assert _classify_frequency(7.0) == "weekly"

    def test_weekly_boundary(self):
        assert _classify_frequency(5.0) == "weekly"
        assert _classify_frequency(10.0) == "weekly"

    def test_monthly(self):
        assert _classify_frequency(30.0) == "monthly"

    def test_monthly_boundary(self):
        assert _classify_frequency(25.0) == "monthly"
        assert _classify_frequency(35.0) == "monthly"

    def test_yearly(self):
        assert _classify_frequency(365.0) == "yearly"

    def test_yearly_boundary(self):
        assert _classify_frequency(340.0) == "yearly"
        assert _classify_frequency(395.0) == "yearly"

    def test_irregular(self):
        assert _classify_frequency(15.0) is None
        assert _classify_frequency(100.0) is None
        assert _classify_frequency(2.0) is None


class TestCoefficientOfVariation:
    def test_uniform_values(self):
        assert _coefficient_of_variation([30.0, 30.0, 30.0]) == pytest.approx(0.0)

    def test_single_value(self):
        assert _coefficient_of_variation([30.0]) == float("inf")

    def test_empty(self):
        assert _coefficient_of_variation([]) == float("inf")

    def test_moderate_variation(self):
        cv = _coefficient_of_variation([28.0, 30.0, 32.0])
        assert 0 < cv < 0.5

    def test_high_variation(self):
        cv = _coefficient_of_variation([5.0, 60.0, 10.0])
        assert cv > 0.5


class TestProcessRecurringRows:
    def _make_row(
        self,
        name: str,
        dates: list[date],
        amount: float = 10.0,
        currency: str = "USD",
    ) -> dict:
        return {
            "merchant_name": name,
            "estimated_amount": amount,
            "occurrences": len(dates),
            "total_amount": amount * len(dates),
            "dates": dates,
            "currency": currency,
        }

    def test_monthly_subscription(self):
        # Regular monthly pattern
        dates = [date(2024, 1, 15), date(2024, 2, 14), date(2024, 3, 16), date(2024, 4, 15)]
        rows = [self._make_row("netflix", dates, 15.99)]

        results = _process_recurring_rows(rows)

        assert len(results) == 1
        assert results[0].frequency == "monthly"
        assert results[0].merchant_name == "netflix"

    def test_weekly_subscription(self):
        base = date(2024, 1, 1)
        dates = [base + timedelta(days=7 * i) for i in range(5)]
        rows = [self._make_row("gym", dates, 20.0)]

        results = _process_recurring_rows(rows)

        assert len(results) == 1
        assert results[0].frequency == "weekly"

    def test_yearly_subscription(self):
        dates = [date(2022, 3, 1), date(2023, 3, 2), date(2024, 2, 28)]
        rows = [self._make_row("insurance", dates, 1200.0)]

        results = _process_recurring_rows(rows)

        assert len(results) == 1
        assert results[0].frequency == "yearly"

    def test_irregular_excluded(self):
        # Irregular intervals — should be excluded
        dates = [date(2024, 1, 1), date(2024, 2, 15), date(2024, 6, 1)]
        rows = [self._make_row("random", dates)]

        results = _process_recurring_rows(rows)
        assert len(results) == 0

    def test_high_cv_excluded(self):
        # Monthly-ish but very inconsistent
        dates = [date(2024, 1, 1), date(2024, 1, 20), date(2024, 3, 15), date(2024, 4, 1)]
        rows = [self._make_row("inconsistent", dates)]

        results = _process_recurring_rows(rows)
        assert len(results) == 0

    def test_single_date_excluded(self):
        rows = [self._make_row("once", [date(2024, 1, 1)])]
        results = _process_recurring_rows(rows)
        assert len(results) == 0

    def test_sorted_by_total_amount_desc(self):
        dates_a = [date(2024, 1, 1) + timedelta(days=30 * i) for i in range(4)]
        dates_b = [date(2024, 1, 1) + timedelta(days=30 * i) for i in range(4)]
        rows = [
            self._make_row("cheap", dates_a, 5.0),
            self._make_row("expensive", dates_b, 50.0),
        ]

        results = _process_recurring_rows(rows)

        assert len(results) == 2
        assert results[0].merchant_name == "expensive"
        assert results[1].merchant_name == "cheap"

    def test_multi_currency(self):
        dates = [date(2024, 1, 1) + timedelta(days=30 * i) for i in range(4)]
        rows = [
            self._make_row("spotify", dates, 9.99, "USD"),
            self._make_row("spotify", dates, 399.0, "UYU"),
        ]

        results = _process_recurring_rows(rows)
        assert len(results) == 2
        currencies = {r.currency for r in results}
        assert currencies == {"USD", "UYU"}


# ── Integration test with mocked connection ───────────────────────


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


class TestGetRecurringExpenses:
    @pytest.mark.asyncio
    async def test_end_to_end_with_mock(self):
        dates = [date(2024, 1, 15), date(2024, 2, 14), date(2024, 3, 16), date(2024, 4, 15)]
        rows = [
            {
                "merchant_name": "netflix",
                "estimated_amount": 15.99,
                "occurrences": 4,
                "total_amount": 63.96,
                "dates": dates,
                "currency": "USD",
            }
        ]

        schema = _make_schema()
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=rows)
        deps = AgentDeps(
            connection=conn,
            query_builder=QueryBuilder(schema),
            schema=schema,
            user_id="user-1",
        )
        ctx = MagicMock(spec=RunContext)
        ctx.deps = deps

        result = await get_recurring_expenses(ctx, date(2024, 1, 1), date(2024, 12, 31))

        assert len(result) == 1
        assert isinstance(result[0], RecurringExpense)
        assert result[0].frequency == "monthly"
        assert result[0].merchant_name == "netflix"

    @pytest.mark.asyncio
    async def test_records_tool_call(self):
        schema = _make_schema()
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[])
        deps = AgentDeps(
            connection=conn,
            query_builder=QueryBuilder(schema),
            schema=schema,
            user_id="user-1",
        )
        ctx = MagicMock(spec=RunContext)
        ctx.deps = deps

        await get_recurring_expenses(ctx, date(2024, 1, 1), date(2024, 12, 31), min_occurrences=5)

        tc = deps.tool_calls[0]
        assert tc.tool_name == "get_recurring_expenses"
        assert tc.parameters["min_occurrences"] == 5
