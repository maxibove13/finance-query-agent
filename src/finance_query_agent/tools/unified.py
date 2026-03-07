"""Unified view tools: query_expenses, query_income, query_balance_history.

These tools query pre-computed materialized views that have pre-joined exchange
rates, excluded internal transfers, and de-duplicated CC payments. They return
amounts in a single currency (USD or local) so the agent can give unified totals.

Availability is config-driven: each tool is hidden via its prepare callback
when the corresponding ViewMapping is not configured in SchemaMapping.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic_ai import RunContext
from pydantic_ai.tools import ToolDefinition

from finance_query_agent.schemas.responses import ToolCallRecord
from finance_query_agent.schemas.unified_results import BalanceSnapshot, ExpenseGroup, IncomeMonth
from finance_query_agent.tools import AgentDeps

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Prepare callbacks — hide tools when view mappings are absent
# ---------------------------------------------------------------------------


async def _prepare_query_expenses(ctx: RunContext[AgentDeps], tool_def: ToolDefinition) -> ToolDefinition | None:
    return tool_def if ctx.deps.schema.unified_expenses is not None else None


async def _prepare_query_income(ctx: RunContext[AgentDeps], tool_def: ToolDefinition) -> ToolDefinition | None:
    return tool_def if ctx.deps.schema.unified_income is not None else None


async def _prepare_query_balance_history(ctx: RunContext[AgentDeps], tool_def: ToolDefinition) -> ToolDefinition | None:
    return tool_def if ctx.deps.schema.unified_balances is not None else None


# ---------------------------------------------------------------------------
# query_expenses
# ---------------------------------------------------------------------------


async def query_expenses(
    ctx: RunContext[AgentDeps],
    period_start: date,
    period_end: date,
    group_by: Literal["category", "month", "merchant", "total"] = "total",
    currency: Literal["usd", "local"] = "usd",
    category: str | None = None,
    merchant: str | None = None,
    limit: int | None = None,
) -> list[ExpenseGroup]:
    """Aggregate expenses over a date range. Amounts are pre-converted to a single currency by the database.

    Internal transfers and credit-card payment double-counting are excluded automatically.

    group_by controls the aggregation dimension:
      'category' — one row per expense category, sorted by amount descending
      'month'    — one row per calendar month (label format: YYYY-MM)
      'merchant' — one row per merchant, sorted by amount descending
      'total'    — single row with the grand total

    Filters (all optional, combine freely):
      category — exact match, case-sensitive
      merchant — substring match, case-insensitive (SQL ILIKE)
      limit    — cap the number of rows returned (applied after sorting)

    Each result includes a 'currency' field confirming whether amounts are USD or local.
    """
    deps = ctx.deps
    view = deps.schema.unified_expenses
    assert view is not None  # guaranteed by prepare callback
    col = view.columns

    amount_col = col["usd_amount"] if currency == "usd" else col["local_amount"]

    group_expr_map = {
        "category": f"COALESCE({col['category']}, 'Uncategorized')",
        "month": f"TO_CHAR({col['date']}, 'YYYY-MM')",
        "merchant": f"COALESCE({col['merchant']}, 'Unknown')",
        "total": "'Total'",
    }
    group_expr = group_expr_map[group_by]

    # Build WHERE clause
    params: list[object] = [deps.user_id, period_start, period_end]
    where_clauses = [
        f"{col['user_id']} = $1",
        f"{col['date']} BETWEEN $2 AND $3",
    ]

    if category is not None:
        params.append(category)
        where_clauses.append(f"{col['category']} = ${len(params)}")

    if merchant is not None:
        params.append(f"%{merchant}%")
        where_clauses.append(f"{col['merchant']} ILIKE ${len(params)}")

    where = " AND ".join(where_clauses)
    limit_clause = f" LIMIT {int(limit)}" if limit is not None else ""

    # For 'total' grouping, no GROUP BY needed (just aggregate)
    if group_by == "total":
        sql = (
            f"SELECT {group_expr} AS label, "
            f"COALESCE(SUM({amount_col}), 0) AS total_amount, "
            f"COUNT(*) AS transaction_count "
            f"FROM {view.table} WHERE {where}"
            f"{limit_clause}"
        )
    else:
        order = "label" if group_by == "month" else "total_amount DESC"
        sql = (
            f"SELECT {group_expr} AS label, "
            f"SUM({amount_col}) AS total_amount, "
            f"COUNT(*) AS transaction_count "
            f"FROM {view.table} WHERE {where} "
            f"GROUP BY label ORDER BY {order}"
            f"{limit_clause}"
        )

    start = time.monotonic()
    try:
        rows = await deps.connection.fetch(sql, *params)
    except Exception:
        logger.error("Tool '%s' query failed | sql=%s", "query_expenses", sql)
        raise
    elapsed_ms = int((time.monotonic() - start) * 1000)

    results = [
        ExpenseGroup(
            label=row["label"],
            total_amount=row["total_amount"],
            transaction_count=row["transaction_count"],
            currency=currency,
        )
        for row in rows
    ]

    deps.tool_calls.append(
        ToolCallRecord(
            tool_name="query_expenses",
            parameters={
                "period_start": str(period_start),
                "period_end": str(period_end),
                "group_by": group_by,
                "currency": currency,
                "category": category,
                "merchant": merchant,
                "limit": limit,
            },
            execution_time_ms=elapsed_ms,
            row_count=len(rows),
        )
    )
    deps.tool_results.append(("query_expenses", results))
    return results


# ---------------------------------------------------------------------------
# query_income
# ---------------------------------------------------------------------------


async def query_income(
    ctx: RunContext[AgentDeps],
    period_start: date,
    period_end: date,
    currency: Literal["usd", "local"] = "usd",
) -> list[IncomeMonth]:
    """Monthly income totals over a date range. Amounts are pre-converted to a single currency by the database.

    Returns one row per month within the range, ordered chronologically (label format: YYYY/MM).
    Each result includes a 'currency' field confirming whether amounts are USD or local.
    """
    deps = ctx.deps
    view = deps.schema.unified_income
    assert view is not None
    col = view.columns

    amount_col = col["usd_amount"] if currency == "usd" else col["local_amount"]

    start_text = f"{period_start.year:04d}/{period_start.month:02d}"
    end_text = f"{period_end.year:04d}/{period_end.month:02d}"

    sql = (
        f"SELECT {col['month']} AS month_label, "
        f"SUM({amount_col}) AS total_amount "
        f"FROM {view.table} "
        f"WHERE {col['user_id']} = $1 AND {col['month']} >= $2 AND {col['month']} <= $3 "
        f"GROUP BY {col['month']} ORDER BY {col['month']}"
    )
    params: list[object] = [deps.user_id, start_text, end_text]

    start = time.monotonic()
    try:
        rows = await deps.connection.fetch(sql, *params)
    except Exception:
        logger.error("Tool '%s' query failed | sql=%s", "query_income", sql)
        raise
    elapsed_ms = int((time.monotonic() - start) * 1000)

    results = [
        IncomeMonth(
            month_label=row["month_label"],
            total_amount=row["total_amount"],
            currency=currency,
        )
        for row in rows
    ]

    deps.tool_calls.append(
        ToolCallRecord(
            tool_name="query_income",
            parameters={
                "period_start": str(period_start),
                "period_end": str(period_end),
                "currency": currency,
            },
            execution_time_ms=elapsed_ms,
            row_count=len(rows),
        )
    )
    deps.tool_results.append(("query_income", results))
    return results


# ---------------------------------------------------------------------------
# query_balance_history
# ---------------------------------------------------------------------------


async def query_balance_history(
    ctx: RunContext[AgentDeps],
    period_start: date | None = None,
    period_end: date | None = None,
    currency: Literal["usd", "local"] = "usd",
    include_breakdown: bool = False,
    granularity: Literal["daily", "monthly"] = "monthly",
) -> list[BalanceSnapshot]:
    """Balance snapshots from the database. Amounts are pre-converted to a single currency.

    No date parameters -> returns only the latest snapshot.
    With dates -> returns snapshots within the range.

    granularity controls time resolution:
      'monthly' — one snapshot per month (last recorded day of each month)
      'daily'   — every recorded snapshot

    include_breakdown=True adds a per-currency JSONB breakdown alongside the single-currency total
    (only works if the view maps a currency_breakdown column; otherwise the field is null).
    """
    deps = ctx.deps
    view = deps.schema.unified_balances
    assert view is not None
    col = view.columns

    has_breakdown = "currency_breakdown" in col
    params: list[object] = [deps.user_id]
    where_clauses = [f"{col['user_id']} = $1"]

    balance_col = col["usd_total"] if currency == "usd" else col["local_total"]

    # No dates -> latest snapshot
    if period_start is None and period_end is None:
        select_parts = [f"{col['date']} AS date", f"{balance_col} AS total_balance"]
        if include_breakdown and has_breakdown:
            select_parts.append(f"{col['currency_breakdown']} AS currency_balances")

        sql = (
            f"SELECT {', '.join(select_parts)} "
            f"FROM {view.table} WHERE {where_clauses[0]} "
            f"ORDER BY {col['date']} DESC LIMIT 1"
        )
    else:
        if period_start is not None:
            params.append(period_start)
            where_clauses.append(f"{col['date']} >= ${len(params)}")
        if period_end is not None:
            params.append(period_end)
            where_clauses.append(f"{col['date']} <= ${len(params)}")

        where = " AND ".join(where_clauses)
        select_parts = [f"{col['date']} AS date", f"{balance_col} AS total_balance"]
        if include_breakdown and has_breakdown:
            select_parts.append(f"{col['currency_breakdown']} AS currency_balances")

        if granularity == "monthly":
            sql = (
                f"SELECT DISTINCT ON (DATE_TRUNC('month', {col['date']})) "
                f"{', '.join(select_parts)} "
                f"FROM {view.table} WHERE {where} "
                f"ORDER BY DATE_TRUNC('month', {col['date']}), {col['date']} DESC"
            )
        else:
            sql = f"SELECT {', '.join(select_parts)} FROM {view.table} WHERE {where} ORDER BY {col['date']}"

    start = time.monotonic()
    try:
        rows = await deps.connection.fetch(sql, *params)
    except Exception:
        logger.error("Tool '%s' query failed | sql=%s", "query_balance_history", sql)
        raise
    elapsed_ms = int((time.monotonic() - start) * 1000)

    results = []
    for row in rows:
        currency_balances = None
        if "currency_balances" in row.keys():
            raw = row["currency_balances"]
            if raw is not None:
                parsed = json.loads(raw) if isinstance(raw, str) else raw
                currency_balances = {k: Decimal(str(v)) for k, v in parsed.items()}
        results.append(
            BalanceSnapshot(
                date=row["date"],
                total_balance=row["total_balance"],
                currency_balances=currency_balances,
            )
        )

    deps.tool_calls.append(
        ToolCallRecord(
            tool_name="query_balance_history",
            parameters={
                "period_start": str(period_start) if period_start else None,
                "period_end": str(period_end) if period_end else None,
                "currency": currency,
                "include_breakdown": include_breakdown,
                "granularity": granularity,
            },
            execution_time_ms=elapsed_ms,
            row_count=len(rows),
        )
    )
    deps.tool_results.append(("query_balance_history", results))
    return results
