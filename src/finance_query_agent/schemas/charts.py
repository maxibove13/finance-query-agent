"""Chart specification models for visualization agent output."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, model_validator


class PieSlice(BaseModel):
    label: str
    value: float
    percentage: float = 0.0  # computed by PieChartSpec validator; not from LLM


class PieChartSpec(BaseModel):
    chart_type: Literal["pie"] = "pie"
    title: str
    currency: str
    slices: list[PieSlice]

    @model_validator(mode="after")
    def compute_percentages(self) -> PieChartSpec:
        total = sum(s.value for s in self.slices)
        if total > 0:
            for s in self.slices:
                s.percentage = round(s.value / total * 100, 2)
        return self


class BarItem(BaseModel):
    label: str
    value: float


class BarChartSpec(BaseModel):
    chart_type: Literal["bar"] = "bar"
    title: str
    currency: str
    bars: list[BarItem]


class LinePoint(BaseModel):
    label: str
    value: float


class LineChartSpec(BaseModel):
    chart_type: Literal["line"] = "line"
    title: str
    currency: str
    points: list[LinePoint]


class GroupedBarItem(BaseModel):
    label: str
    value_a: float
    value_b: float


class GroupedBarChartSpec(BaseModel):
    chart_type: Literal["grouped_bar"] = "grouped_bar"
    title: str
    currency: str
    groups: list[GroupedBarItem]
    series_labels: list[str] = Field(min_length=2, max_length=2)


ChartSpec = Annotated[
    PieChartSpec | BarChartSpec | LineChartSpec | GroupedBarChartSpec,
    Field(discriminator="chart_type"),
]
