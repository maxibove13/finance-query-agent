"""Agent tools package — AgentDeps dataclass shared across all tools."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from finance_query_agent.connection import Connection
from finance_query_agent.schemas.responses import ToolCallRecord


@dataclass
class AgentDeps:
    connection: Connection
    user_id: Any
    tool_calls: list[ToolCallRecord] = field(default_factory=list)
    tool_results: list[tuple[str, Any]] = field(default_factory=list)
