"""Visualization agent — generates Vega-Lite chart specs from query tool results."""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel
from pydantic_ai import Agent

from finance_query_agent.schemas.charts import ChartIntent, VegaLiteChart
from finance_query_agent.vega_builder import build_vega_spec

logger = logging.getLogger(__name__)

CHARTABLE_TOOLS = frozenset({"execute_sql"})

_SYSTEM_PROMPT = """\
You are a financial data visualization agent. Given a user's original question and \
the structured SQL query results, produce chart intents.

## Available chart types

- **bar**: Categorical comparison (spending by category, by merchant).
- **line**: Trends over time (monthly spending, balance history).
- **pie**: Category proportions / breakdowns (max 8 slices; combine smallest into "Other").
- **area**: Cumulative trends over time.
- **scatter**: Correlation between two numeric fields.
- **heatmap**: Intensity across two dimensions (e.g., day-of-week × category).
- **stacked_bar**: Category breakdown within groups (e.g., categories per month).
- **grouped_bar**: Side-by-side comparison across groups.

## Rules

1. Produce ONE chart per currency. If data spans USD and UYU, return two chart intents.
2. `x_field` and `y_field` MUST be actual column names from the query results.
3. Use `color_field` when a third dimension adds value (e.g., category within monthly bars).
4. Title should be short and descriptive (e.g., "Spending by Category (USD)").
5. Return an EMPTY charts list if:
   - The data has only 1 row (a chart adds no value over the text answer)
   - The data is not meaningfully chartable
6. Values must be positive floats (use absolute values for expenses).
7. For bar/line charts, set `sort` to order data logically (chronological → "none", ranked → "descending").
"""


class _VisualizationOutputLLM(BaseModel):
    charts: list[ChartIntent]


# ---------------------------------------------------------------------------

_viz_agents: dict[str, Agent[None, _VisualizationOutputLLM]] = {}


def _get_viz_agent(model: str) -> Agent[None, _VisualizationOutputLLM]:
    if model in _viz_agents:
        return _viz_agents[model]

    agent: Agent[None, _VisualizationOutputLLM] = Agent(
        model,
        output_type=_VisualizationOutputLLM,
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


def _collect_data_rows(tool_results: list[tuple[str, Any]]) -> list[dict[str, Any]]:
    """Extract dict rows from the *last* chartable tool result.

    Only the final execute_sql call is used so that earlier exploratory queries
    (which may have different columns or scopes) don't pollute the chart data.
    """
    last_data: Any = None
    for tool_name, data in tool_results:
        if tool_name in CHARTABLE_TOOLS:
            last_data = data

    if last_data is None:
        return []

    rows: list[dict[str, Any]] = []
    if isinstance(last_data, list):
        for item in last_data:
            if hasattr(item, "model_dump"):
                rows.append(item.model_dump(mode="json"))
            elif isinstance(item, dict):
                rows.append(item)
    elif hasattr(last_data, "model_dump"):
        rows.append(last_data.model_dump(mode="json"))
    elif isinstance(last_data, dict):
        rows.append(last_data)
    return rows


def _filter_rows_for_intent(intent: ChartIntent, data_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filter data rows by intent's currency, if specified.

    Matches against any column whose value equals the target currency string,
    so the filter works regardless of column aliasing.
    """
    if not intent.currency:
        return data_rows
    currency_lower = intent.currency.lower()
    filtered = [r for r in data_rows if any(isinstance(v, str) and v.lower() == currency_lower for v in r.values())]
    # If nothing matched (column missing or aliased away), return all rows
    # rather than silently producing an empty chart.
    return filtered if filtered else data_rows


def _intent_fields_exist(intent: ChartIntent, rows: list[dict[str, Any]]) -> bool:
    """Return True if every field referenced by *intent* exists in *rows*."""
    if not rows:
        return False
    available = set(rows[0].keys())
    required = {intent.x_field, intent.y_field}
    if intent.color_field:
        required.add(intent.color_field)
    return required.issubset(available)


def _serialize_tool_results(tool_results: list[tuple[str, Any]]) -> str:
    """Serialize only the *last* chartable tool result for the viz model.

    This matches ``_collect_data_rows`` which also uses only the last result,
    so the viz model cannot reference columns from earlier (stale) queries.
    """
    last_name: str | None = None
    last_data: Any = None
    for tool_name, data in tool_results:
        if tool_name in CHARTABLE_TOOLS:
            last_name = tool_name
            last_data = data

    if last_name is None:
        return ""

    if isinstance(last_data, list):
        rows = [item.model_dump(mode="json") if hasattr(item, "model_dump") else item for item in last_data]
    elif hasattr(last_data, "model_dump"):
        rows = last_data.model_dump(mode="json")
    else:
        rows = last_data

    columns = ""
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        columns = f"\nColumns: {', '.join(rows[0].keys())}"

    return f"Tool: {last_name}{columns}\nData: {rows}"


async def generate_visualizations(
    question: str,
    tool_results: list[tuple[str, Any]],
    model: str = "openai:gpt-4.1-mini",
) -> list[VegaLiteChart] | None:
    """Run the visualization agent and return Vega-Lite chart specs, or None."""
    if not should_visualize(tool_results):
        return None

    serialized = _serialize_tool_results(tool_results)
    if not serialized:
        return None

    prompt = f"User question: {question}\n\n{serialized}"

    try:
        agent = _get_viz_agent(model)
        result = await agent.run(prompt)
        intents = result.output.charts
        if not intents:
            return None

        data_rows = _collect_data_rows(tool_results)
        charts = []
        for intent in intents:
            try:
                rows = _filter_rows_for_intent(intent, data_rows)
                if not rows or not _intent_fields_exist(intent, rows):
                    continue
                charts.append(VegaLiteChart(spec=build_vega_spec(intent, rows)))
            except Exception:
                logger.warning("Skipping invalid chart intent: %s", intent.chart_type, exc_info=True)
                continue
        return charts if charts else None
    except Exception:
        logger.exception("Visualization agent failed")
        return None
