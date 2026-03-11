"""Single SQL execution tool for the finance query agent."""

from __future__ import annotations

import time
from typing import Any

from pydantic_ai import RunContext

from finance_query_agent.schemas.responses import ToolCallRecord
from finance_query_agent.sql_governance import validate_select_only
from finance_query_agent.tools import AgentDeps


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

    result = [dict(row) for row in rows]

    ctx.deps.tool_calls.append(
        ToolCallRecord(
            tool_name="execute_sql",
            parameters={"sql": sql},
            execution_time_ms=elapsed_ms,
            row_count=len(result),
        )
    )
    ctx.deps.tool_results.append(("execute_sql", result))

    return result
