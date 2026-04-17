"""Render tools — generative UI components for the frontend.

Each tool validates its input via Pydantic and appends a RenderCall to deps.
The handler collects render_calls into the Lambda response so the frontend
can match component names to React components.
"""

from __future__ import annotations

from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError
from pydantic_ai import ModelRetry, RunContext

from finance_query_agent.schemas.charts import (
    BubbleChartData,
    CashFlowData,
    CashFlowHistoricalData,
    CategoryBreakdownData,
    DonutChartData,
    MetricCardData,
    RenderCall,
)
from finance_query_agent.tools import AgentDeps


def _record(ctx: RunContext[AgentDeps], component: str, data: dict[str, Any]) -> None:
    ctx.deps.render_calls.append(RenderCall(component=component, data=data))


_T = TypeVar("_T", bound=BaseModel)


def _validate(model_cls: type[_T], **kwargs: Any) -> _T:
    """Construct a Pydantic model, converting ValidationError → ModelRetry."""
    try:
        return model_cls(**kwargs)
    except ValidationError as exc:
        raise ModelRetry(f"Invalid render data: {exc}") from exc


async def render_donut_chart(
    ctx: RunContext[AgentDeps],
    period: str,
    currency: str,
    slices: list[dict[str, Any]],
) -> str:
    """Render a donut chart showing distribution by category (% of total).

    Use when the user asks about spending/income breakdown by category.
    Each slice needs category, value, and optionally emoji.
    Requires at least 2 slices.
    """
    data = _validate(DonutChartData, period=period, currency=currency, slices=slices)
    _record(ctx, "donut_chart", data.model_dump())
    return f"Donut chart rendered: {period} ({len(data.slices)} slices)"


async def render_bubble_chart(
    ctx: RunContext[AgentDeps],
    period: str,
    currency: str,
    bubbles: list[dict[str, Any]],
) -> str:
    """Render a bubble chart comparing magnitudes across categories.

    Use for comparing absolute amounts across categories when relative size matters.
    Each bubble needs category, value, and optionally emoji.
    Requires at least 2 bubbles.
    """
    data = _validate(BubbleChartData, period=period, currency=currency, bubbles=bubbles)
    _record(ctx, "bubble_chart", data.model_dump())
    return f"Bubble chart rendered: {period} ({len(data.bubbles)} bubbles)"


async def render_cash_flow(
    ctx: RunContext[AgentDeps],
    period: str,
    currency: str,
    income: float,
    expenses: float,
) -> str:
    """Render a cash flow chart showing income vs expenses for a single period.

    Use when the user asks about their balance, net income, or income vs expenses
    for a specific month or period.
    """
    data = _validate(CashFlowData, period=period, currency=currency, income=income, expenses=expenses)
    _record(ctx, "cash_flow", data.model_dump())
    return f"Cash flow rendered: {period} (income={income}, expenses={expenses})"


async def render_cash_flow_historical(
    ctx: RunContext[AgentDeps],
    currency: str,
    series: list[dict[str, Any]],
) -> str:
    """Render a historical cash flow time series chart.

    Use when the user asks about income/expense trends over multiple months.
    Each entry needs period, income, and expenses.
    Requires at least 2 periods.
    """
    data = _validate(CashFlowHistoricalData, currency=currency, series=series)
    _record(ctx, "cash_flow_historical", data.model_dump())
    return f"Historical cash flow rendered: {len(data.series)} periods"


async def render_category_breakdown(
    ctx: RunContext[AgentDeps],
    period: str,
    currency: str,
    categories: list[dict[str, Any]],
) -> str:
    """Render a detailed category breakdown table.

    Use when the user wants a detailed list of spending/income per category
    with amounts and percentages. Each row needs category, amount, and
    optionally emoji and pct.
    """
    data = _validate(CategoryBreakdownData, period=period, currency=currency, categories=categories)
    _record(ctx, "category_breakdown", data.model_dump())
    return f"Category breakdown rendered: {period} ({len(data.categories)} categories)"


async def render_metric_card(
    ctx: RunContext[AgentDeps],
    label: str,
    value: float,
    currency: str | None = None,
    period: str | None = None,
    change_pct: float | None = None,
) -> str:
    """Render a single KPI metric card.

    Use for scalar answers like total spending, average transaction, count, etc.
    Optionally include currency, period, and change_pct vs previous period.
    """
    data = _validate(MetricCardData, label=label, value=value, currency=currency, period=period, change_pct=change_pct)
    _record(ctx, "metric_card", data.model_dump())
    suffix = f" {currency}" if currency else ""
    return f"Metric card rendered: {label} = {value}{suffix}"


RENDER_TOOLS = [
    render_donut_chart,
    render_bubble_chart,
    render_cash_flow,
    render_cash_flow_historical,
    render_category_breakdown,
    render_metric_card,
]
