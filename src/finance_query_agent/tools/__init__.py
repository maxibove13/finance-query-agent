"""Agent tools package — AgentDeps dataclass shared across all tools."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from finance_query_agent.connection import Connection
from finance_query_agent.query_builder import QueryBuilder
from finance_query_agent.schemas.mapping import SchemaMapping
from finance_query_agent.schemas.responses import ToolCallRecord


@dataclass
class AgentDeps:
    connection: Connection
    query_builder: QueryBuilder
    schema: SchemaMapping
    user_id: Any
    tool_calls: list[ToolCallRecord] = field(default_factory=list)
    tool_results: list[tuple[str, Any]] = field(default_factory=list)
    fallback_used: bool = False
    fallback_sql: str | None = None
