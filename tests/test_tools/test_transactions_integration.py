"""Integration tests for transaction tools — runs against seeded testcontainers Postgres."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest
from pydantic_ai import RunContext

from finance_query_agent.connection import Connection
from finance_query_agent.query_builder import QueryBuilder
from finance_query_agent.schemas.mapping import SchemaMapping
from finance_query_agent.schemas.tool_results import Transaction
from finance_query_agent.tools import AgentDeps
from finance_query_agent.tools.transactions import search_transactions

SEED_USER_1 = 1


def _make_ctx(conn: Connection, qb: QueryBuilder, schema: SchemaMapping, user_id: str) -> RunContext[AgentDeps]:
    deps = AgentDeps(connection=conn, query_builder=qb, schema=schema, user_id=user_id)
    ctx = MagicMock(spec=RunContext)
    ctx.deps = deps
    return ctx


class TestSearchTransactionsIntegration:
    @pytest.mark.asyncio
    async def test_returns_transactions(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await search_transactions(ctx, period_start=date(2025, 10, 1), period_end=date(2025, 10, 31))

        assert result.total_count > 0
        assert all(isinstance(t, Transaction) for t in result.transactions)

    @pytest.mark.asyncio
    async def test_text_search(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await search_transactions(ctx, query="Netflix")

        assert result.total_count > 0
        assert all("Netflix" in t.description for t in result.transactions)

    @pytest.mark.asyncio
    async def test_direction_expense(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await search_transactions(
            ctx, period_start=date(2025, 10, 1), period_end=date(2025, 10, 31), direction="expense"
        )

        # Should not contain salary (credit)
        descriptions = [t.description for t in result.transactions]
        assert "Salary Oct" not in descriptions
        assert result.total_count == 4  # 4 debits in Oct

    @pytest.mark.asyncio
    async def test_direction_income(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await search_transactions(
            ctx, period_start=date(2025, 10, 1), period_end=date(2025, 10, 31), direction="income"
        )

        assert result.total_count == 1
        assert result.transactions[0].description == "Salary Oct"

    @pytest.mark.asyncio
    async def test_amount_range(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await search_transactions(ctx, min_amount=100.0, max_amount=200.0)

        for t in result.transactions:
            assert abs(t.amount) >= 100
            assert abs(t.amount) <= 200

    @pytest.mark.asyncio
    async def test_pagination(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        page1 = await search_transactions(ctx, limit=5, offset=0)
        page2 = await search_transactions(ctx, limit=5, offset=5)

        assert len(page1.transactions) == 5
        if page1.total_count > 5:
            assert page1.has_more is True
        # Pages should not overlap
        dates1 = {(t.date, t.description) for t in page1.transactions}
        dates2 = {(t.date, t.description) for t in page2.transactions}
        assert dates1.isdisjoint(dates2)

    @pytest.mark.asyncio
    async def test_category_filter(self, db_connection, query_builder, sample_schema_mapping):
        ctx = _make_ctx(db_connection, query_builder, sample_schema_mapping, SEED_USER_1)

        result = await search_transactions(ctx, category="entertainment")

        assert result.total_count > 0
        assert all(t.category == "entertainment" for t in result.transactions)
