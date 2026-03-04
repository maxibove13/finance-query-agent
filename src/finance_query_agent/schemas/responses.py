"""Agent response models."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class ToolCallRecord(BaseModel):
    tool_name: str
    parameters: dict[str, Any]
    execution_time_ms: int
    row_count: int


class TokenUsage(BaseModel):
    input_tokens: int
    output_tokens: int


class AgentResponse(BaseModel):
    answer: str
    tool_calls: list[ToolCallRecord]
    fallback_used: bool
    fallback_sql: str | None
    unresolved: bool
    original_question: str
    token_usage: TokenUsage
