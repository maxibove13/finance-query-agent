"""Tests for visualization agent — should_visualize, serialization, Vega-Lite output."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

from finance_query_agent.schemas.charts import ChartIntent, VegaLiteChart
from finance_query_agent.vega_builder import VEGA_LITE_SCHEMA, build_vega_spec
from finance_query_agent.visualization import (
    _chartable_row_count,
    _filter_rows_for_intent,
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

    def test_includes_column_names(self):
        result = _serialize_tool_results([("execute_sql", _TWO_EXPENSE_ROWS)])
        assert "Columns:" in result
        assert "category" in result

    def test_skips_non_chartable_tools(self):
        result = _serialize_tool_results([("other_tool", _TWO_EXPENSE_ROWS)])
        assert result == ""

    def test_serializes_only_last_execute_sql_call(self):
        results = [
            ("execute_sql", _TWO_EXPENSE_ROWS),
            ("execute_sql", [{"month": "2025-01", "total": 3000.0}]),
        ]
        result = _serialize_tool_results(results)
        # Only the last result is serialized
        assert result.count("execute_sql") == 1
        assert "month" in result
        assert "Food" not in result

    def test_empty_data_still_serializes(self):
        result = _serialize_tool_results([("execute_sql", [])])
        assert "execute_sql" in result
        assert "[]" in result


# -- ChartIntent model -------------------------------------------------------


class TestFilterRowsForIntent:
    def test_filter_rows_for_intent_with_currency(self):
        intent = ChartIntent(
            chart_type="bar", title="USD Spending", x_field="category", y_field="total", currency="USD"
        )
        rows = [
            {"category": "Food", "total": 100.0, "currency": "USD"},
            {"category": "Transport", "total": 50.0, "currency": "UYU"},
            {"category": "Rent", "total": 800.0, "currency": "USD"},
        ]
        result = _filter_rows_for_intent(intent, rows)
        assert len(result) == 2
        assert all(r["currency"] == "USD" for r in result)

    def test_filter_rows_for_intent_without_currency(self):
        intent = ChartIntent(chart_type="bar", title="All Spending", x_field="category", y_field="total")
        rows = [
            {"category": "Food", "total": 100.0, "currency": "USD"},
            {"category": "Transport", "total": 50.0, "currency": "UYU"},
        ]
        result = _filter_rows_for_intent(intent, rows)
        assert len(result) == 2

    def test_filter_rows_case_insensitive(self):
        intent = ChartIntent(chart_type="bar", title="Test", x_field="x", y_field="y", currency="usd")
        rows = [{"x": 1, "y": 2, "currency": "USD"}]
        result = _filter_rows_for_intent(intent, rows)
        assert len(result) == 1

    def test_filter_rows_aliased_currency_column(self):
        """Currency match works even if the column isn't named 'currency'."""
        intent = ChartIntent(chart_type="bar", title="Test", x_field="x", y_field="y", currency="USD")
        rows = [{"x": 1, "y": 2, "cur": "USD"}, {"x": 3, "y": 4, "cur": "UYU"}]
        result = _filter_rows_for_intent(intent, rows)
        assert len(result) == 1
        assert result[0]["cur"] == "USD"

    def test_filter_rows_falls_back_when_no_match(self):
        """If currency is set but no row matches, return all rows instead of empty."""
        intent = ChartIntent(chart_type="bar", title="Test", x_field="x", y_field="y", currency="EUR")
        rows = [{"x": 1, "y": 2}]
        result = _filter_rows_for_intent(intent, rows)
        assert len(result) == 1


class TestIntentFieldsExist:
    def test_valid_fields(self):
        from finance_query_agent.visualization import _intent_fields_exist

        intent = ChartIntent(chart_type="bar", title="T", x_field="category", y_field="total")
        assert _intent_fields_exist(intent, _TWO_EXPENSE_ROWS) is True

    def test_missing_field(self):
        from finance_query_agent.visualization import _intent_fields_exist

        intent = ChartIntent(chart_type="bar", title="T", x_field="missing", y_field="total")
        assert _intent_fields_exist(intent, _TWO_EXPENSE_ROWS) is False

    def test_missing_color_field(self):
        from finance_query_agent.visualization import _intent_fields_exist

        intent = ChartIntent(chart_type="bar", title="T", x_field="category", y_field="total", color_field="nope")
        assert _intent_fields_exist(intent, _TWO_EXPENSE_ROWS) is False

    def test_empty_rows(self):
        from finance_query_agent.visualization import _intent_fields_exist

        intent = ChartIntent(chart_type="bar", title="T", x_field="x", y_field="y")
        assert _intent_fields_exist(intent, []) is False


class TestChartIntent:
    def test_accepts_all_chart_types(self):
        for ct in ("bar", "line", "pie", "area", "scatter", "heatmap", "stacked_bar", "grouped_bar"):
            intent = ChartIntent(chart_type=ct, title="Test", x_field="x", y_field="y")
            assert intent.chart_type == ct

    def test_optional_fields_default(self):
        intent = ChartIntent(chart_type="bar", title="Test", x_field="x", y_field="y")
        assert intent.currency is None
        assert intent.color_field is None
        assert intent.sort == "none"
        assert intent.series_labels is None


# -- build_vega_spec ---------------------------------------------------------


class TestBuildVegaSpec:
    def _intent(self, **kwargs) -> ChartIntent:
        defaults = {"chart_type": "bar", "title": "Test Chart", "x_field": "category", "y_field": "total"}
        defaults.update(kwargs)
        return ChartIntent(**defaults)

    def test_bar_chart(self):
        spec = build_vega_spec(self._intent(), _TWO_EXPENSE_ROWS)
        assert spec["$schema"] == VEGA_LITE_SCHEMA
        assert spec["mark"] == "bar"
        assert spec["encoding"]["x"]["field"] == "category"
        assert spec["encoding"]["y"]["field"] == "total"
        assert spec["data"]["values"] == _TWO_EXPENSE_ROWS

    def test_bar_chart_descending_sort(self):
        spec = build_vega_spec(self._intent(sort="descending"), _TWO_EXPENSE_ROWS)
        assert spec["encoding"]["x"]["sort"] == "-y"

    def test_bar_chart_ascending_sort(self):
        spec = build_vega_spec(self._intent(sort="ascending"), _TWO_EXPENSE_ROWS)
        assert spec["encoding"]["x"]["sort"] == "y"

    def test_bar_chart_no_sort(self):
        spec = build_vega_spec(self._intent(sort="none"), _TWO_EXPENSE_ROWS)
        assert "sort" not in spec["encoding"]["x"]

    def test_bar_with_color_field(self):
        spec = build_vega_spec(self._intent(color_field="currency"), _TWO_EXPENSE_ROWS)
        assert spec["encoding"]["color"]["field"] == "currency"

    def test_line_chart(self):
        spec = build_vega_spec(self._intent(chart_type="line", x_field="month"), _TWO_EXPENSE_ROWS)
        assert spec["mark"] == {"type": "line", "point": True}
        assert spec["encoding"]["x"]["type"] == "ordinal"

    def test_pie_chart(self):
        spec = build_vega_spec(self._intent(chart_type="pie"), _TWO_EXPENSE_ROWS)
        assert spec["mark"] == {"type": "arc"}
        assert "theta" in spec["encoding"]
        assert spec["encoding"]["theta"]["field"] == "total"
        assert spec["encoding"]["color"]["field"] == "category"

    def test_area_chart(self):
        spec = build_vega_spec(self._intent(chart_type="area", x_field="month"), _TWO_EXPENSE_ROWS)
        assert spec["mark"] == "area"

    def test_scatter_chart(self):
        spec = build_vega_spec(self._intent(chart_type="scatter", x_field="amount", y_field="count"), _TWO_EXPENSE_ROWS)
        assert spec["mark"] == "point"
        assert spec["encoding"]["x"]["type"] == "quantitative"

    def test_heatmap(self):
        spec = build_vega_spec(
            self._intent(chart_type="heatmap", x_field="day", y_field="hour", color_field="amount"),
            _TWO_EXPENSE_ROWS,
        )
        assert spec["mark"] == "rect"
        assert spec["encoding"]["y"]["field"] == "hour"
        assert spec["encoding"]["color"]["field"] == "amount"

    def test_stacked_bar(self):
        spec = build_vega_spec(
            self._intent(chart_type="stacked_bar", color_field="subcategory"),
            _TWO_EXPENSE_ROWS,
        )
        assert spec["mark"] == "bar"
        assert spec["encoding"]["color"]["field"] == "subcategory"

    def test_grouped_bar(self):
        spec = build_vega_spec(
            self._intent(chart_type="grouped_bar", color_field="period"),
            _TWO_EXPENSE_ROWS,
        )
        assert spec["mark"] == "bar"
        assert "xOffset" in spec["encoding"]
        assert spec["encoding"]["xOffset"]["field"] == "period"

    def test_grouped_bar_without_color_field_raises(self):
        import pytest

        with pytest.raises(ValueError, match="grouped_bar requires color_field"):
            build_vega_spec(self._intent(chart_type="grouped_bar"), _TWO_EXPENSE_ROWS)

    def test_data_embedded_inline(self):
        data = [{"a": 1}, {"a": 2}]
        spec = build_vega_spec(self._intent(), data)
        assert spec["data"]["values"] is data

    def test_theme_applied(self):
        spec = build_vega_spec(self._intent(), _TWO_EXPENSE_ROWS)
        assert "config" in spec

    def test_title_from_intent(self):
        spec = build_vega_spec(self._intent(title="My Custom Title"), _TWO_EXPENSE_ROWS)
        assert spec["title"] == "My Custom Title"


# -- AgentResponse with VegaLiteChart -----------------------------------------


class TestAgentResponseWithVegaLite:
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

    def test_response_with_vega_lite_chart(self):
        from finance_query_agent.schemas.responses import AgentResponse, TokenUsage

        chart = VegaLiteChart(
            spec={
                "$schema": VEGA_LITE_SCHEMA,
                "title": "Test",
                "mark": "bar",
                "data": {"values": []},
                "encoding": {},
            }
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
        assert "$schema" in dumped["visualizations"][0]["spec"]

    def test_response_serialization_roundtrip(self):
        from finance_query_agent.schemas.responses import AgentResponse, TokenUsage

        chart = VegaLiteChart(
            spec=build_vega_spec(
                ChartIntent(chart_type="bar", title="Monthly", x_field="month", y_field="total"),
                [{"month": "2026/01", "total": 500.0}],
            )
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
        assert restored.visualizations[0].spec["mark"] == "bar"


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
