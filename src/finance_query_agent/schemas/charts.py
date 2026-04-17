"""Component data models for generative UI render tools."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

# -- Donut chart (distribution by category, % of total) ----------------------


class DonutSlice(BaseModel):
    category: str
    value: float
    emoji: str | None = None


class DonutChartData(BaseModel):
    period: str = Field(description="Human-readable period label, e.g. '2026-03' or 'Last 30 days'.")
    currency: str
    slices: list[DonutSlice] = Field(min_length=2)


# -- Bubble chart (magnitude comparison by category) -------------------------


class Bubble(BaseModel):
    category: str
    value: float
    emoji: str | None = None


class BubbleChartData(BaseModel):
    period: str
    currency: str
    bubbles: list[Bubble] = Field(min_length=2)


# -- Cash flow (income vs expenses in a single period) -----------------------


class CashFlowData(BaseModel):
    period: str
    currency: str
    income: float
    expenses: float


# -- Cash flow historical (time series of income vs expenses) ----------------


class CashFlowPeriod(BaseModel):
    period: str
    income: float
    expenses: float


class CashFlowHistoricalData(BaseModel):
    currency: str
    series: list[CashFlowPeriod] = Field(min_length=2)


# -- Category breakdown (detailed table-like breakdown) ----------------------


class CategoryRow(BaseModel):
    category: str
    amount: float
    emoji: str | None = None
    pct: float | None = Field(default=None, description="Percentage of total.")


class CategoryBreakdownData(BaseModel):
    period: str
    currency: str
    categories: list[CategoryRow] = Field(min_length=1)


# -- Metric card (single KPI) ------------------------------------------------


class MetricCardData(BaseModel):
    label: str
    value: float
    currency: str | None = None
    period: str | None = None
    change_pct: float | None = Field(default=None, description="Change vs previous period, e.g. -12.5.")


# -- Render call (component name + serialized data) --------------------------


class RenderCall(BaseModel):
    """A render call returned to the frontend. The frontend matches *component*
    to a React component and passes *data* as props."""

    component: str
    data: dict[str, Any]
