"""Single SQL execution tool for the finance query agent."""

from __future__ import annotations

import datetime
import decimal
import hashlib
import time
from typing import Any

import logfire
from pydantic_ai import RunContext

from finance_query_agent.schemas.responses import ToolCallRecord
from finance_query_agent.sql_governance import validate_select_only
from finance_query_agent.tools import AgentDeps


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    """Convert asyncpg-specific types to JSON-safe equivalents."""
    return {k: _normalize_value(v) for k, v in row.items()}


def _normalize_value(v: Any) -> Any:
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    return v


async def execute_sql(ctx: RunContext[AgentDeps], sql: str) -> list[dict[str, Any]]:
    """Execute a SELECT query against the financial database and return the results.

    Generates and runs any SELECT query against the schema provided in the system prompt.
    Only SELECT statements are permitted — write operations are rejected.
    User data is automatically scoped via Row Level Security.
    """
    validate_select_only(sql)

    start = time.monotonic()
    rows = await ctx.deps.connection.execute_governed(sql, ctx.deps.user_id)
    elapsed_ms = int((time.monotonic() - start) * 1000)

    result = [_normalize_row(dict(row)) for row in rows]

    ctx.deps.tool_calls.append(
        ToolCallRecord(
            tool_name="execute_sql",
            parameters={"sql": sql},
            execution_time_ms=elapsed_ms,
            row_count=len(result),
        )
    )
    ctx.deps.tool_results.append(("execute_sql", result))

    logfire.info(
        "sql_query",
        sql_hash=hashlib.sha256(sql.encode()).hexdigest()[:16],
        row_count=len(result),
        empty_result=len(result) == 0,
        execution_time_ms=elapsed_ms,
    )

    return result
