"""Trend tools: compare_periods, get_spending_trend, get_category_breakdown."""

from __future__ import annotations

import time
from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic_ai import RunContext

from finance_query_agent.schemas.responses import ToolCallRecord
from finance_query_agent.schemas.tool_results import (
    CategoryBreakdown,
    PeriodComparison,
    TrendPoint,
)
from finance_query_agent.tools import AgentDeps


async def compare_periods(
    ctx: RunContext[AgentDeps],
    period_a_start: date,
    period_a_end: date,
    period_b_start: date,
    period_b_end: date,
    group_by: Literal["category", "merchant", "total"] = "total",
) -> list[PeriodComparison]:
    """Compare spending between two time periods. Only counts expenses."""
    deps = ctx.deps
    query = deps.query_builder.build_compare_periods(
        user_id=deps.user_id,
        period_a_start=period_a_start,
        period_a_end=period_a_end,
        period_b_start=period_b_start,
        period_b_end=period_b_end,
        group_by=group_by,
    )

    start = time.monotonic()
    rows = await deps.connection.fetch(query.sql, *query.params)
    elapsed_ms = int((time.monotonic() - start) * 1000)

    results = []
    for row in rows:
        a_total = Decimal(str(row["period_a_total"]))
        b_total = Decimal(str(row["period_b_total"]))
        absolute_change = b_total - a_total
        if a_total != 0:
            percentage_change = float((absolute_change / a_total) * 100)
        else:
            percentage_change = None

        results.append(
            PeriodComparison(
                group_label=row["group_label"],
                currency=row["currency"],
                period_a_total=a_total,
                period_b_total=b_total,
                absolute_change=absolute_change,
                percentage_change=percentage_change,
            )
        )

    deps.tool_calls.append(
        ToolCallRecord(
            tool_name="compare_periods",
            parameters={
                "period_a_start": str(period_a_start),
                "period_a_end": str(period_a_end),
                "period_b_start": str(period_b_start),
                "period_b_end": str(period_b_end),
                "group_by": group_by,
            },
            execution_time_ms=elapsed_ms,
            row_count=len(rows),
        )
    )
    return results


async def get_spending_trend(
    ctx: RunContext[AgentDeps],
    period_start: date,
    period_end: date,
    granularity: Literal["week", "month"] = "month",
    category: str | None = None,
) -> list[TrendPoint]:
    """Get spending over time bucketed by week or month. Only counts expenses."""
    deps = ctx.deps
    query = deps.query_builder.build_spending_trend(
        user_id=deps.user_id,
        period_start=period_start,
        period_end=period_end,
        granularity=granularity,
        category=category,
    )

    start = time.monotonic()
    rows = await deps.connection.fetch(query.sql, *query.params)
    elapsed_ms = int((time.monotonic() - start) * 1000)

    results = [
        TrendPoint(
            period_label=row["period_label"],
            total_amount=row["total_amount"],
            transaction_count=row["transaction_count"],
            currency=row["currency"],
        )
        for row in rows
    ]

    deps.tool_calls.append(
        ToolCallRecord(
            tool_name="get_spending_trend",
            parameters={
                "period_start": str(period_start),
                "period_end": str(period_end),
                "granularity": granularity,
                "category": category,
            },
            execution_time_ms=elapsed_ms,
            row_count=len(rows),
        )
    )
    return results


async def get_category_breakdown(
    ctx: RunContext[AgentDeps],
    period_start: date,
    period_end: date,
    account_id: str | None = None,
) -> list[CategoryBreakdown]:
    """Get percentage breakdown of spending by category. Results per currency."""
    deps = ctx.deps
    query = deps.query_builder.build_category_breakdown(
        user_id=deps.user_id,
        period_start=period_start,
        period_end=period_end,
        account_id=account_id,
    )

    start = time.monotonic()
    rows = await deps.connection.fetch(query.sql, *query.params)
    elapsed_ms = int((time.monotonic() - start) * 1000)

    results = [
        CategoryBreakdown(
            category=row["category"],
            total_amount=row["total_amount"],
            percentage=float(row["percentage"]),
            currency=row["currency"],
        )
        for row in rows
    ]

    deps.tool_calls.append(
        ToolCallRecord(
            tool_name="get_category_breakdown",
            parameters={
                "period_start": str(period_start),
                "period_end": str(period_end),
                "account_id": account_id,
            },
            execution_time_ms=elapsed_ms,
            row_count=len(rows),
        )
    )
    return results
