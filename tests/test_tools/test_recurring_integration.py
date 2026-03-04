"""Integration tests for recurring expense tool — runs against seeded testcontainers Postgres."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest
from pydantic_ai import RunContext

from finance_query_agent.connection import Connection
from finance_query_agent.query_builder import QueryBuilder
from finance_query_agent.schemas.mapping import SchemaMapping
from finance_query_agent.schemas.tool_results import RecurringExpense
from finance_query_agent.tools import AgentDeps
from finance_query_agent.tools.recurring import get_recurring_expenses

SEED_USER_1 = "test-user-1"


def _make_ctx(conn: Connection, qb: QueryBuilder, schema: SchemaMapping, user_id: str) -> RunContext[AgentDeps]:
    deps = AgentDeps(connection=conn, query_builder=qb, schema=schema, user_id=user_id)
    ctx = MagicMock(spec=RunContext)
    ctx.deps = deps
    return ctx


class TestGetRecurringExpensesIntegration:
    @pytest.mark.asyncio
    async def test_detects_netflix(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        # Netflix appears monthly: Oct 20, Nov 20, Dec 20, Jan 20, Feb 20 = 5 occurrences
        result = await get_recurring_expenses(ctx, date(2025, 10, 1), date(2026, 2, 28), min_occurrences=3)

        netflix = [r for r in result if "netflix" in r.merchant_name.lower()]
        assert len(netflix) == 1
        assert isinstance(netflix[0], RecurringExpense)
        assert netflix[0].frequency == "monthly"

    @pytest.mark.asyncio
    async def test_returns_empty_for_short_period(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        # Single month won't have enough occurrences
        result = await get_recurring_expenses(ctx, date(2025, 10, 1), date(2025, 10, 31), min_occurrences=3)

        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_sorted_by_total_desc(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await get_recurring_expenses(ctx, date(2025, 10, 1), date(2026, 2, 28), min_occurrences=3)

        for i in range(len(result) - 1):
            assert result[i].total_amount >= result[i + 1].total_amount
