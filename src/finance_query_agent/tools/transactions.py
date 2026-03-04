"""Transaction tools: search_transactions, get_top_merchants."""

from __future__ import annotations

import time
from datetime import date
from typing import Literal

from pydantic_ai import RunContext

from finance_query_agent.schemas.responses import ToolCallRecord
from finance_query_agent.schemas.tool_results import (
    MerchantSpending,
    Transaction,
    TransactionSearchResult,
)
from finance_query_agent.tools import AgentDeps


async def search_transactions(
    ctx: RunContext[AgentDeps],
    query: str | None = None,
    period_start: date | None = None,
    period_end: date | None = None,
    min_amount: float | None = None,
    max_amount: float | None = None,
    category: str | None = None,
    direction: Literal["expense", "income"] | None = None,
    limit: int = 20,
    offset: int = 0,
) -> TransactionSearchResult:
    """Search transactions by description, amount, date, or category. Returns all directions unless filtered."""
    deps = ctx.deps
    data_query, count_query = deps.query_builder.build_search_transactions(
        user_id=deps.user_id,
        query=query,
        period_start=period_start,
        period_end=period_end,
        min_amount=min_amount,
        max_amount=max_amount,
        category=category,
        direction=direction,
        limit=limit,
        offset=offset,
    )

    start = time.monotonic()
    rows = await deps.connection.fetch(data_query.sql, *data_query.params)
    count_row = await deps.connection.fetchrow(count_query.sql, *count_query.params)
    elapsed_ms = int((time.monotonic() - start) * 1000)

    total_count = count_row["total_count"] if count_row else 0

    transactions = [
        Transaction(
            date=row["date"],
            amount=row["amount"],
            description=row["description"],
            currency=row["currency"],
            category=row["category"],
        )
        for row in rows
    ]

    result = TransactionSearchResult(
        transactions=transactions,
        total_count=total_count,
        has_more=total_count > offset + limit,
    )

    deps.tool_calls.append(
        ToolCallRecord(
            tool_name="search_transactions",
            parameters={
                "query": query,
                "period_start": str(period_start) if period_start else None,
                "period_end": str(period_end) if period_end else None,
                "min_amount": min_amount,
                "max_amount": max_amount,
                "category": category,
                "direction": direction,
                "limit": limit,
                "offset": offset,
            },
            execution_time_ms=elapsed_ms,
            row_count=len(rows),
        )
    )
    return result


async def get_top_merchants(
    ctx: RunContext[AgentDeps],
    period_start: date,
    period_end: date,
    limit: int = 10,
    category: str | None = None,
) -> list[MerchantSpending]:
    """Get top merchants by spending. Only counts expenses."""
    deps = ctx.deps
    query = deps.query_builder.build_top_merchants(
        user_id=deps.user_id,
        period_start=period_start,
        period_end=period_end,
        limit=limit,
        category=category,
    )

    start = time.monotonic()
    rows = await deps.connection.fetch(query.sql, *query.params)
    elapsed_ms = int((time.monotonic() - start) * 1000)

    results = [
        MerchantSpending(
            merchant_name=row["merchant_name"],
            total_amount=row["total_amount"],
            transaction_count=row["transaction_count"],
            currency=row["currency"],
        )
        for row in rows
    ]

    deps.tool_calls.append(
        ToolCallRecord(
            tool_name="get_top_merchants",
            parameters={
                "period_start": str(period_start),
                "period_end": str(period_end),
                "limit": limit,
                "category": category,
            },
            execution_time_ms=elapsed_ms,
            row_count=len(rows),
        )
    )
    return results
