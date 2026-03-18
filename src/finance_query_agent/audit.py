"""DynamoDB SQL audit logging."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import boto3  # type: ignore[import-untyped]

from finance_query_agent.redaction import redact_pii

if TYPE_CHECKING:
    from finance_query_agent.schemas.responses import TokenUsage, ToolCallRecord


class SqlAudit:
    def __init__(self, table_name: str, region: str) -> None:
        self._dynamo = boto3.resource("dynamodb", region_name=region)
        self._table = self._dynamo.Table(table_name)

    async def write_invocation(
        self,
        *,
        user_id: str | int,
        session_id: str,
        question: str,
        tool_calls: list[ToolCallRecord],
        token_usage: TokenUsage,
        total_ms: int,
        unresolved: bool,
    ) -> None:
        now = datetime.now(UTC)
        uid_short = uuid.uuid4().hex[:8]
        ttl = int((now + timedelta(days=90)).timestamp())

        sql_queries = [
            {
                "sql": redact_pii(tc.parameters.get("sql", "")),
                "row_count": tc.row_count,
                "execution_time_ms": tc.execution_time_ms,
                "empty_result": tc.row_count == 0,
            }
            for tc in tool_calls
        ]

        item = {
            "PK": f"USER#{user_id}",
            "SK": f"INVOCATION#{now.isoformat()}#{uid_short}",
            "user_id": str(user_id),
            "session_id": session_id,
            "question": redact_pii(question),
            "sql_queries": sql_queries,
            "query_count": len(tool_calls),
            "retried": len(tool_calls) > 1,
            "any_empty_result": any(tc.row_count == 0 for tc in tool_calls),
            "total_execution_ms": total_ms,
            "input_tokens": token_usage.input_tokens,
            "output_tokens": token_usage.output_tokens,
            "unresolved": unresolved,
            "created_at": now.isoformat(),
            "ttl": ttl,
        }

        await asyncio.to_thread(self._table.put_item, Item=item)
