"""Transaction tools: search_transactions."""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Literal

from pydantic_ai import RunContext

from finance_query_agent.schemas.responses import ToolCallRecord
from finance_query_agent.schemas.tool_results import (
    Transaction,
    TransactionSearchResult,
)
from finance_query_agent.tools import AgentDeps

logger = logging.getLogger(__name__)


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
    """Search individual transactions by text, amount range, date range, category, or direction.

    Returns per-currency rows — each row includes its original currency code.
    Supports pagination: use limit/offset to page through results. Response includes total_count and has_more.
    All directions (expense + income) are returned unless direction is explicitly filtered.
    query does a case-insensitive substring match on the transaction description.
    """
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
    try:
        rows = await deps.connection.fetch(data_query.sql, *data_query.params)
    except Exception:
        logger.error("Tool '%s' query failed | sql=%s", "search_transactions", data_query.sql)
        raise
    try:
        count_row = await deps.connection.fetchrow(count_query.sql, *count_query.params)
    except Exception:
        logger.error("Tool '%s' count query failed | sql=%s", "search_transactions", count_query.sql)
        raise
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
    deps.tool_results.append(("search_transactions", result))
    return result
