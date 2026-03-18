"""Chart intent and Vega-Lite chart models."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel


class ChartIntent(BaseModel):
    """Simplified chart intent — what the LLM outputs."""

    chart_type: Literal["bar", "line", "pie", "area", "scatter", "heatmap", "stacked_bar", "grouped_bar"]
    title: str
    currency: str | None = None
    x_field: str
    y_field: str
    color_field: str | None = None
    sort: Literal["ascending", "descending", "none"] = "none"
    series_labels: list[str] | None = None


class VegaLiteChart(BaseModel):
    """Wrapper around a full Vega-Lite v5 JSON spec."""

    spec: dict[str, Any]
