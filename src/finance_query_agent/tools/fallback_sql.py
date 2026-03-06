"""Constrained SQL fallback tool — handles queries not covered by predefined tools."""

from __future__ import annotations

import logging
import re
import time
from typing import Any

from pydantic_ai import ModelRetry, RunContext

from finance_query_agent.redaction import sanitize_error
from finance_query_agent.schemas.responses import ToolCallRecord
from finance_query_agent.tools import AgentDeps
from finance_query_agent.validation.sql_validator import SqlValidator

logger = logging.getLogger(__name__)

# Matches user_id conditions the LLM might have generated.
# We strip these and replace with the SDK-controlled one.
_USER_ID_CONDITION_RE = re.compile(
    r"\b(?:\w+\.)?(?:user_id|\"user_id\")\s*=\s*(?:\$\d+|'[^']*'|\"[^\"]*\"|\d+)\s*(?:AND\s+)?",
    re.IGNORECASE,
)


def _strip_llm_user_filter(sql: str) -> str:
    """Remove any user_id conditions the LLM may have added."""
    result = _USER_ID_CONDITION_RE.sub("", sql)
    # Clean up dangling WHERE with no conditions
    result = re.sub(r"\bWHERE\s+(GROUP|ORDER|LIMIT|HAVING|$)", r"\1", result, flags=re.IGNORECASE)
    # Clean up trailing AND
    result = re.sub(r"\bAND\s+(GROUP|ORDER|LIMIT|HAVING|$)", r"\1", result, flags=re.IGNORECASE)
    return result


async def run_constrained_query(ctx: RunContext[AgentDeps], sql: str) -> list[dict[str, Any]]:
    """Execute a constrained SQL query against the financial database.

    Use this tool ONLY when no predefined tool can answer the question.
    The SQL must be a single SELECT statement referencing only mapped tables and columns.
    No CTEs, subqueries, or DML. User scoping is injected automatically — do NOT add user_id filters.
    """
    deps = ctx.deps
    validator = SqlValidator(deps.schema)

    # 1. Validate SQL structure
    errors = validator.validate(sql)
    if errors:
        raise ModelRetry(f"SQL validation failed: {'; '.join(errors)}")

    # 2. Inject LIMIT
    sql = validator.inject_limit(sql)

    # 3. Strip LLM user_id conditions, inject SDK-controlled one
    sql = _strip_llm_user_filter(sql)
    # $1 is reserved for user_id — the SDK controls this parameter
    sql = validator.inject_user_filter(sql, "$1")

    # 4. EXPLAIN validation
    try:
        await deps.connection.fetch(f"EXPLAIN {sql}", deps.user_id)
    except Exception as exc:
        raise ModelRetry(f"EXPLAIN failed: {sanitize_error(exc)}") from exc

    # 5. Execute
    start = time.monotonic()
    try:
        rows = await deps.connection.fetch(sql, deps.user_id)
    except Exception as exc:
        logger.error("Fallback SQL execution failed: %s | SQL: %s", exc, sql)
        raise ModelRetry(f"Query execution failed: {sanitize_error(exc)}") from exc
    elapsed_ms = int((time.monotonic() - start) * 1000)

    result = [dict(row) for row in rows]

    # 6. Record tool call metadata
    deps.fallback_used = True
    deps.fallback_sql = sql
    deps.tool_calls.append(
        ToolCallRecord(
            tool_name="run_constrained_query",
            parameters={"sql": sql},
            execution_time_ms=elapsed_ms,
            row_count=len(result),
        )
    )

    deps.tool_results.append(("run_constrained_query", result))

    # 7. Audit log
    logger.info(
        "Fallback SQL executed | rows=%d | time_ms=%d | sql=%s",
        len(result),
        elapsed_ms,
        sql,
    )

    return result
