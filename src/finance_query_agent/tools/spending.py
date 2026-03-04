"""Spending tools: get_spending_by_category, get_monthly_totals, get_balance_summary."""

from __future__ import annotations

import time
from datetime import date

from pydantic_ai import RunContext
from pydantic_ai.tools import ToolDefinition

from finance_query_agent.schemas.responses import ToolCallRecord
from finance_query_agent.schemas.tool_results import (
    AccountSummary,
    CategorySpending,
    MonthlyTotal,
)
from finance_query_agent.tools import AgentDeps


async def get_spending_by_category(
    ctx: RunContext[AgentDeps],
    period_start: date,
    period_end: date,
    categories: list[str] | None = None,
    account_id: str | None = None,
) -> list[CategorySpending]:
    """Get total spending per category within a time period. Results grouped by currency."""
    deps = ctx.deps
    query = deps.query_builder.build_spending_by_category(
        user_id=deps.user_id,
        period_start=period_start,
        period_end=period_end,
        categories=categories,
        account_id=account_id,
    )
    start = time.monotonic()
    rows = await deps.connection.fetch(query.sql, *query.params)
    elapsed_ms = int((time.monotonic() - start) * 1000)

    results = [
        CategorySpending(
            category=row["category"],
            total_amount=row["total_amount"],
            transaction_count=row["transaction_count"],
            currency=row["currency"],
        )
        for row in rows
    ]

    deps.tool_calls.append(
        ToolCallRecord(
            tool_name="get_spending_by_category",
            parameters={
                "period_start": str(period_start),
                "period_end": str(period_end),
                "categories": categories,
                "account_id": account_id,
            },
            execution_time_ms=elapsed_ms,
            row_count=len(rows),
        )
    )
    return results


async def get_monthly_totals(
    ctx: RunContext[AgentDeps],
    period_start: date,
    period_end: date,
    account_id: str | None = None,
) -> list[MonthlyTotal]:
    """Get aggregated expense totals per month. Results grouped by currency."""
    deps = ctx.deps
    query = deps.query_builder.build_monthly_totals(
        user_id=deps.user_id,
        period_start=period_start,
        period_end=period_end,
        account_id=account_id,
    )
    start = time.monotonic()
    rows = await deps.connection.fetch(query.sql, *query.params)
    elapsed_ms = int((time.monotonic() - start) * 1000)

    results = [
        MonthlyTotal(
            year=row["year"],
            month=row["month"],
            total_amount=row["total_amount"],
            transaction_count=row["transaction_count"],
            currency=row["currency"],
        )
        for row in rows
    ]

    deps.tool_calls.append(
        ToolCallRecord(
            tool_name="get_monthly_totals",
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


async def get_balance_summary(
    ctx: RunContext[AgentDeps],
    account_id: str | None = None,
) -> list[AccountSummary]:
    """Get the most recent balance per account. Only available when balance column is mapped."""
    deps = ctx.deps
    query = deps.query_builder.build_balance_summary(
        user_id=deps.user_id,
        account_id=account_id,
    )
    start = time.monotonic()
    rows = await deps.connection.fetch(query.sql, *query.params)
    elapsed_ms = int((time.monotonic() - start) * 1000)

    results = [
        AccountSummary(
            account_name=row["account_name"],
            latest_balance=row["latest_balance"],
            last_transaction_date=row["last_transaction_date"],
            currency=row["currency"],
        )
        for row in rows
    ]

    deps.tool_calls.append(
        ToolCallRecord(
            tool_name="get_balance_summary",
            parameters={"account_id": account_id},
            execution_time_ms=elapsed_ms,
            row_count=len(rows),
        )
    )
    return results


async def _prepare_balance_summary(
    ctx: RunContext[AgentDeps],
    tool_def: ToolDefinition,
) -> ToolDefinition | None:
    """Hide get_balance_summary when balance column is not mapped."""
    if "balance" in ctx.deps.schema.transactions.columns:
        return tool_def
    return None
