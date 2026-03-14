"""Build Vega-Lite v5 specs from ChartIntent + data rows."""

from __future__ import annotations

from typing import Any

from finance_query_agent.schemas.charts import ChartIntent

VEGA_LITE_SCHEMA = "https://vega.github.io/schema/vega-lite/v5.json"

# Consistent theme applied to all generated charts.
_THEME: dict[str, Any] = {
    "config": {
        "font": "Inter, system-ui, sans-serif",
        "axis": {"labelFontSize": 12, "titleFontSize": 13},
        "title": {"fontSize": 14, "anchor": "start"},
        "legend": {"labelFontSize": 12, "titleFontSize": 13},
        "view": {"stroke": None},
    },
}


def build_vega_spec(intent: ChartIntent, data: list[dict[str, Any]]) -> dict[str, Any]:
    """Convert a ChartIntent + data rows into a full Vega-Lite v5 spec."""
    spec: dict[str, Any] = {
        "$schema": VEGA_LITE_SCHEMA,
        "title": intent.title,
        "data": {"values": data},
        **_THEME,
    }

    builder = _BUILDERS.get(intent.chart_type)
    if builder is None:
        msg = f"Unsupported chart_type: {intent.chart_type}"
        raise ValueError(msg)

    builder(spec, intent)
    return spec


# ---------------------------------------------------------------------------
# Per-chart-type builders — each mutates *spec* in place.
# ---------------------------------------------------------------------------


def _sort_encoding(intent: ChartIntent) -> str | None:
    """Return Vega-Lite sort value for the x encoding."""
    if intent.sort == "descending":
        return "-y"
    if intent.sort == "ascending":
        return "y"
    return None


def _build_bar(spec: dict[str, Any], intent: ChartIntent) -> None:
    spec["mark"] = "bar"
    x_enc: dict[str, Any] = {"field": intent.x_field, "type": "nominal"}
    sort = _sort_encoding(intent)
    if sort:
        x_enc["sort"] = sort
    spec["encoding"] = {
        "x": x_enc,
        "y": {"field": intent.y_field, "type": "quantitative"},
    }
    if intent.color_field:
        spec["encoding"]["color"] = {"field": intent.color_field, "type": "nominal"}


def _build_line(spec: dict[str, Any], intent: ChartIntent) -> None:
    spec["mark"] = {"type": "line", "point": True}
    spec["encoding"] = {
        "x": {"field": intent.x_field, "type": "ordinal"},
        "y": {"field": intent.y_field, "type": "quantitative"},
    }
    if intent.color_field:
        spec["encoding"]["color"] = {"field": intent.color_field, "type": "nominal"}


def _build_pie(spec: dict[str, Any], intent: ChartIntent) -> None:
    spec["mark"] = {"type": "arc"}
    spec["encoding"] = {
        "theta": {"field": intent.y_field, "type": "quantitative"},
        "color": {"field": intent.x_field, "type": "nominal"},
    }


def _build_area(spec: dict[str, Any], intent: ChartIntent) -> None:
    spec["mark"] = "area"
    spec["encoding"] = {
        "x": {"field": intent.x_field, "type": "ordinal"},
        "y": {"field": intent.y_field, "type": "quantitative"},
    }
    if intent.color_field:
        spec["encoding"]["color"] = {"field": intent.color_field, "type": "nominal"}


def _build_scatter(spec: dict[str, Any], intent: ChartIntent) -> None:
    spec["mark"] = "point"
    spec["encoding"] = {
        "x": {"field": intent.x_field, "type": "quantitative"},
        "y": {"field": intent.y_field, "type": "quantitative"},
    }
    if intent.color_field:
        spec["encoding"]["color"] = {"field": intent.color_field, "type": "nominal"}


def _build_heatmap(spec: dict[str, Any], intent: ChartIntent) -> None:
    spec["mark"] = "rect"
    spec["encoding"] = {
        "x": {"field": intent.x_field, "type": "ordinal"},
        "y": {"field": intent.y_field, "type": "ordinal"},
        "color": {"field": intent.color_field or intent.y_field, "type": "quantitative"},
    }


def _build_stacked_bar(spec: dict[str, Any], intent: ChartIntent) -> None:
    spec["mark"] = "bar"
    x_enc: dict[str, Any] = {"field": intent.x_field, "type": "nominal"}
    sort = _sort_encoding(intent)
    if sort:
        x_enc["sort"] = sort
    spec["encoding"] = {
        "x": x_enc,
        "y": {"field": intent.y_field, "type": "quantitative"},
        "color": {"field": intent.color_field or intent.x_field, "type": "nominal"},
    }


def _build_grouped_bar(spec: dict[str, Any], intent: ChartIntent) -> None:
    if not intent.color_field:
        msg = "grouped_bar requires color_field to identify the grouping dimension"
        raise ValueError(msg)
    spec["mark"] = "bar"
    spec["encoding"] = {
        "x": {"field": intent.x_field, "type": "nominal"},
        "xOffset": {"field": intent.color_field, "type": "nominal"},
        "y": {"field": intent.y_field, "type": "quantitative"},
        "color": {"field": intent.color_field, "type": "nominal"},
    }


_BUILDERS: dict[str, Any] = {
    "bar": _build_bar,
    "line": _build_line,
    "pie": _build_pie,
    "area": _build_area,
    "scatter": _build_scatter,
    "heatmap": _build_heatmap,
    "stacked_bar": _build_stacked_bar,
    "grouped_bar": _build_grouped_bar,
}
