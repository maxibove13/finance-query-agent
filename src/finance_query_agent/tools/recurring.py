"""Recurring expense detection tool."""

from __future__ import annotations

import logging
import statistics
import time
from datetime import date
from decimal import Decimal
from typing import Any

from pydantic_ai import RunContext

from finance_query_agent.schemas.responses import ToolCallRecord
from finance_query_agent.schemas.tool_results import RecurringExpense
from finance_query_agent.tools import AgentDeps

logger = logging.getLogger(__name__)

# Frequency classification ranges (median interval in days)
_FREQUENCY_RANGES: list[tuple[int, int, str]] = [
    (5, 10, "weekly"),
    (25, 35, "monthly"),
    (340, 395, "yearly"),
]

_MAX_CV = 0.5  # Maximum coefficient of variation for regularity


def _classify_frequency(median_interval: float) -> str | None:
    """Classify frequency from median interval in days. Returns None for irregular."""
    for lo, hi, label in _FREQUENCY_RANGES:
        if lo <= median_interval <= hi:
            return label
    return None


def _coefficient_of_variation(values: list[float]) -> float:
    """Compute CV = stdev / mean. Returns inf if mean is 0 or single value."""
    if len(values) < 2:
        return float("inf")
    mean = statistics.mean(values)
    if mean == 0:
        return float("inf")
    return statistics.stdev(values) / mean


def _process_recurring_rows(rows: list[Any]) -> list[RecurringExpense]:
    """Post-process SQL results: compute intervals, classify frequency, filter by CV."""
    results: list[RecurringExpense] = []

    for row in rows:
        dates: list[date] = row["dates"]
        if len(dates) < 2:
            continue

        # Compute intervals between consecutive dates
        intervals = [(dates[i + 1] - dates[i]).days for i in range(len(dates) - 1)]
        intervals_float = [float(i) for i in intervals]

        median_interval = statistics.median(intervals_float)
        frequency = _classify_frequency(median_interval)

        if frequency is None:
            continue  # Irregular — exclude

        cv = _coefficient_of_variation(intervals_float)
        if cv > _MAX_CV:
            continue  # Too inconsistent

        results.append(
            RecurringExpense(
                merchant_name=row["merchant_name"],
                estimated_amount=Decimal(str(row["estimated_amount"])),
                frequency=frequency,
                occurrences=row["occurrences"],
                total_amount=Decimal(str(row["total_amount"])),
                currency=row["currency"],
            )
        )

    # Sort by total_amount descending
    results.sort(key=lambda r: r.total_amount, reverse=True)
    return results


async def get_recurring_expenses(
    ctx: RunContext[AgentDeps],
    period_start: date,
    period_end: date,
    min_occurrences: int = 3,
) -> list[RecurringExpense]:
    """Identify recurring transactions (subscriptions, regular payments). Only counts expenses."""
    deps = ctx.deps
    query = deps.query_builder.build_recurring_expenses(
        user_id=deps.user_id,
        period_start=period_start,
        period_end=period_end,
        min_occurrences=min_occurrences,
    )

    start = time.monotonic()
    try:
        rows = await deps.connection.fetch(query.sql, *query.params)
    except Exception:
        logger.error("Tool '%s' query failed | sql=%s", "get_recurring_expenses", query.sql)
        raise
    elapsed_ms = int((time.monotonic() - start) * 1000)

    results = _process_recurring_rows(rows)

    deps.tool_calls.append(
        ToolCallRecord(
            tool_name="get_recurring_expenses",
            parameters={
                "period_start": str(period_start),
                "period_end": str(period_end),
                "min_occurrences": min_occurrences,
            },
            execution_time_ms=elapsed_ms,
            row_count=len(rows),
        )
    )
    deps.tool_results.append(("get_recurring_expenses", results))
    return results
