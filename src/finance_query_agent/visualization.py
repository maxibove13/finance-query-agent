"""Visualization agent — generates chart specs from query tool results."""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel
from pydantic_ai import Agent

from finance_query_agent.schemas.charts import ChartSpec

logger = logging.getLogger(__name__)

CHARTABLE_TOOLS = frozenset({"query_expenses", "query_income", "query_balance_history"})

_SYSTEM_PROMPT = """\
You are a financial data visualization agent. Given a user's original question and \
the structured data returned by query tools, produce chart specifications.

## Available chart types

### pie
Category proportions (spending by category, breakdowns).
- slices: list of {label, value, percentage (0-100)}
- Good for: query_expenses (group_by=category)

### bar
Categorical or time-series comparison.
- bars: list of {label, value}
- Good for: query_expenses (group_by=month or merchant), query_income

### line
Trends over time.
- points: list of {label, value} where label is a time period
- Good for: query_expenses (group_by=month), query_balance_history (multiple snapshots)

### grouped_bar
Side-by-side period comparison.
- groups: list of {label, value_a, value_b}
- series_labels: tuple of two period names (e.g., ["Jan 2026", "Feb 2026"])
- Good for: comparing two query_expenses results with different date ranges

## Rules

1. Produce ONE chart per currency. If data spans USD and UYU, return two charts.
2. For pie charts: max 8 slices. If more categories exist, combine the smallest into "Other". \
Percentages must sum to ~100.
3. Title should be short and descriptive (e.g., "Spending by Category (USD)").
4. Return an EMPTY charts list if:
   - The data has only 1 row (a chart adds no value over the text answer)
   - The data is not meaningfully chartable
5. Values must be positive floats (use absolute values for expenses).
6. For bar/line charts, order data logically (chronological for time, descending for ranked).
"""


class VisualizationOutput(BaseModel):
    """Structured output of the visualization agent."""

    charts: list[ChartSpec]


_viz_agents: dict[str, Agent[None, VisualizationOutput]] = {}


def _get_viz_agent(model: str) -> Agent[None, VisualizationOutput]:
    if model in _viz_agents:
        return _viz_agents[model]

    agent: Agent[None, VisualizationOutput] = Agent(
        model,
        output_type=VisualizationOutput,
        system_prompt=_SYSTEM_PROMPT,
    )
    _viz_agents[model] = agent
    return agent


def _chartable_row_count(tool_results: list[tuple[str, Any]]) -> int:
    """Count total rows across chartable tool results."""
    total = 0
    for name, data in tool_results:
        if name not in CHARTABLE_TOOLS:
            continue
        if isinstance(data, list):
            total += len(data)
        else:
            total += 1
    return total


def should_visualize(tool_results: list[tuple[str, Any]]) -> bool:
    """Return True if any tool result is chartable with enough data."""
    return _chartable_row_count(tool_results) >= 2


def _serialize_tool_results(tool_results: list[tuple[str, Any]]) -> str:
    """Serialize tool results into a prompt-friendly format."""
    parts: list[str] = []
    for tool_name, data in tool_results:
        if tool_name not in CHARTABLE_TOOLS:
            continue
        if isinstance(data, list):
            rows = [item.model_dump(mode="json") if hasattr(item, "model_dump") else item for item in data]
        elif hasattr(data, "model_dump"):
            rows = data.model_dump(mode="json")
        else:
            rows = data
        parts.append(f"Tool: {tool_name}\nData: {rows}")
    return "\n\n".join(parts)


async def generate_visualizations(
    question: str,
    tool_results: list[tuple[str, Any]],
    model: str = "openai:gpt-4o-mini",
) -> list[ChartSpec] | None:
    """Run the visualization agent and return chart specs, or None."""
    if not should_visualize(tool_results):
        return None

    serialized = _serialize_tool_results(tool_results)
    if not serialized:
        return None

    prompt = f"User question: {question}\n\n{serialized}"

    try:
        agent = _get_viz_agent(model)
        result = await agent.run(prompt)
        charts = result.output.charts
        return charts if charts else None
    except Exception:
        logger.exception("Visualization agent failed")
        return None
