"""Agent response models."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from finance_query_agent.schemas.charts import RenderCall


class AgentOutput(BaseModel):
    """Structured output from the query agent — text answer only.

    Visualization is handled by render tools that the agent calls directly,
    not by a separate field on the output.
    """

    answer: str


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
    render_calls: list[RenderCall]
    unresolved: bool
    original_question: str
    token_usage: TokenUsage
