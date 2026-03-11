"""Tests for visualization agent — should_visualize, serialization, and chart spec output."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

from finance_query_agent.schemas.charts import (
    BarChartSpec,
    GroupedBarChartSpec,
    LineChartSpec,
    PieChartSpec,
)
from finance_query_agent.visualization import (
    _chartable_row_count,
    _serialize_tool_results,
    generate_visualizations,
    should_visualize,
)

# -- sample data for execute_sql results ------------------------------------

_TWO_EXPENSE_ROWS = [
    {"category": "Food", "total": 100.0, "currency": "USD"},
    {"category": "Transport", "total": 50.0, "currency": "USD"},
]

_ONE_ROW = [{"category": "Food", "total": 100.0, "currency": "USD"}]


# -- should_visualize --------------------------------------------------------


class TestShouldVisualize:
    def test_returns_true_for_execute_sql_with_enough_rows(self):
        assert should_visualize([("execute_sql", _TWO_EXPENSE_ROWS)]) is True

    def test_returns_false_for_single_row(self):
        assert should_visualize([("execute_sql", _ONE_ROW)]) is False

    def test_returns_false_for_empty(self):
        assert should_visualize([]) is False

    def test_returns_false_for_empty_data(self):
        assert should_visualize([("execute_sql", [])]) is False

    def test_rows_accumulate_across_multiple_execute_sql_calls(self):
        results = [
            ("execute_sql", _ONE_ROW),
            ("execute_sql", _ONE_ROW),
        ]
        assert should_visualize(results) is True

    def test_non_chartable_tool_not_counted(self):
        # only execute_sql is chartable; a tool called something else is not
        assert should_visualize([("other_tool", _TWO_EXPENSE_ROWS)]) is False


class TestChartableRowCount:
    def test_counts_list_items(self):
        assert _chartable_row_count([("execute_sql", _TWO_EXPENSE_ROWS)]) == 2

    def test_counts_non_list_as_one(self):
        assert _chartable_row_count([("execute_sql", "scalar")]) == 1

    def test_ignores_non_chartable(self):
        assert _chartable_row_count([("other_tool", ["a", "b", "c"])]) == 0

    def test_sums_across_multiple_calls(self):
        results = [
            ("execute_sql", _ONE_ROW),
            ("execute_sql", _TWO_EXPENSE_ROWS),
        ]
        assert _chartable_row_count(results) == 3


# -- _serialize_tool_results -------------------------------------------------


class TestSerializeToolResults:
    def test_serializes_dict_rows(self):
        result = _serialize_tool_results([("execute_sql", _TWO_EXPENSE_ROWS)])
        assert "execute_sql" in result
        assert "Food" in result
        assert "100" in result

    def test_skips_non_chartable_tools(self):
        result = _serialize_tool_results([("other_tool", _TWO_EXPENSE_ROWS)])
        assert result == ""

    def test_handles_multiple_execute_sql_calls(self):
        results = [
            ("execute_sql", _TWO_EXPENSE_ROWS),
            ("execute_sql", [{"month": "2025-01", "total": 3000.0}]),
        ]
        result = _serialize_tool_results(results)
        assert result.count("execute_sql") == 2

    def test_empty_data_still_serializes(self):
        result = _serialize_tool_results([("execute_sql", [])])
        assert "execute_sql" in result
        assert "[]" in result


# -- Chart spec model validation ---------------------------------------------


class TestChartSpecModels:
    def test_pie_chart_spec(self):
        chart = PieChartSpec(
            title="Spending by Category (USD)",
            currency="USD",
            slices=[
                {"label": "Food", "value": 150.0},
                {"label": "Transport", "value": 100.0},
            ],
        )
        assert chart.chart_type == "pie"
        assert len(chart.slices) == 2
        dumped = chart.model_dump()
        assert dumped["chart_type"] == "pie"

    def test_pie_percentages_computed_from_values(self):
        """Validator computes correct percentages regardless of what the LLM output."""
        chart = PieChartSpec(
            title="Spending by Category",
            currency="ARS",
            slices=[
                {"label": "Transferencias", "value": 18346.00, "percentage": 60.82},
                {"label": "Fijos", "value": 12196.00, "percentage": 41.14},
                {"label": "Otros", "value": 1707.54, "percentage": 5.76},
                {"label": "Restaurantes", "value": 1111.48, "percentage": 3.73},
            ],
        )
        total = sum(s.value for s in chart.slices)
        for s in chart.slices:
            assert abs(s.percentage - round(s.value / total * 100, 2)) < 0.01
        assert abs(sum(s.percentage for s in chart.slices) - 100.0) < 0.1

    def test_bar_chart_spec(self):
        chart = BarChartSpec(
            title="Monthly Spending (USD)",
            currency="USD",
            bars=[
                {"label": "2026/01", "value": 500.0},
                {"label": "2026/02", "value": 350.0},
            ],
        )
        assert chart.chart_type == "bar"
        assert len(chart.bars) == 2

    def test_line_chart_spec(self):
        chart = LineChartSpec(
            title="Spending Trend (USD)",
            currency="USD",
            points=[
                {"label": "2025/10", "value": 300.0},
                {"label": "2025/11", "value": 350.0},
                {"label": "2025/12", "value": 280.0},
            ],
        )
        assert chart.chart_type == "line"
        assert len(chart.points) == 3

    def test_grouped_bar_chart_spec(self):
        chart = GroupedBarChartSpec(
            title="Oct vs Nov Spending (USD)",
            currency="USD",
            groups=[
                {"label": "Food", "value_a": 200.0, "value_b": 180.0},
                {"label": "Transport", "value_a": 100.0, "value_b": 120.0},
            ],
            series_labels=("Oct 2025", "Nov 2025"),
        )
        assert chart.chart_type == "grouped_bar"
        assert len(chart.groups) == 2
        assert chart.series_labels == ["Oct 2025", "Nov 2025"]


# -- AgentResponse with visualizations ---------------------------------------


class TestAgentResponseVisualization:
    def test_response_without_visualizations(self):
        from finance_query_agent.schemas.responses import AgentResponse, TokenUsage

        resp = AgentResponse(
            answer="test",
            tool_calls=[],
            unresolved=False,
            original_question="test",
            token_usage=TokenUsage(input_tokens=0, output_tokens=0),
        )
        assert resp.visualizations is None
        dumped = resp.model_dump()
        assert dumped["visualizations"] is None

    def test_response_with_visualizations(self):
        from finance_query_agent.schemas.responses import AgentResponse, TokenUsage

        chart = PieChartSpec(
            title="Test",
            currency="USD",
            slices=[{"label": "A", "value": 100.0}],
        )
        resp = AgentResponse(
            answer="test",
            tool_calls=[],
            visualizations=[chart],
            unresolved=False,
            original_question="test",
            token_usage=TokenUsage(input_tokens=0, output_tokens=0),
        )
        assert len(resp.visualizations) == 1
        dumped = resp.model_dump()
        assert dumped["visualizations"][0]["chart_type"] == "pie"

    def test_response_serialization_roundtrip(self):
        """Ensure chart specs survive JSON serialization/deserialization."""
        from finance_query_agent.schemas.responses import AgentResponse, TokenUsage

        chart = BarChartSpec(
            title="Monthly",
            currency="USD",
            bars=[{"label": "2026/01", "value": 500.0}],
        )
        resp = AgentResponse(
            answer="test",
            tool_calls=[],
            visualizations=[chart],
            unresolved=False,
            original_question="test",
            token_usage=TokenUsage(input_tokens=0, output_tokens=0),
        )
        json_str = resp.model_dump_json()
        restored = AgentResponse.model_validate_json(json_str)
        assert len(restored.visualizations) == 1
        assert restored.visualizations[0].chart_type == "bar"


# -- generate_visualizations edge cases --------------------------------------


class TestGenerateVisualizations:
    def test_returns_none_for_non_chartable(self):
        result = asyncio.run(
            generate_visualizations("query", [("other_tool", ["a", "b"])]),
        )
        assert result is None

    def test_returns_none_for_single_row(self):
        result = asyncio.run(
            generate_visualizations("query", [("execute_sql", _ONE_ROW)]),
        )
        assert result is None

    def test_timeout_returns_none(self):
        """Viz call that exceeds timeout should return None when wrapped in wait_for."""

        async def _slow_viz():
            await asyncio.sleep(10)

        with patch("finance_query_agent.visualization._get_viz_agent") as mock_get:
            mock_agent = AsyncMock()
            mock_agent.run = _slow_viz
            mock_get.return_value = mock_agent

            async def _run():
                try:
                    return await asyncio.wait_for(
                        generate_visualizations("spending?", [("execute_sql", _TWO_EXPENSE_ROWS)]),
                        timeout=0.1,
                    )
                except TimeoutError:
                    return None

            result = asyncio.run(_run())
            assert result is None
