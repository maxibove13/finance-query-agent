"""Tests for visualization agent — should_visualize, serialization, and chart spec output."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from finance_query_agent.schemas.charts import (
    BarChartSpec,
    GroupedBarChartSpec,
    LineChartSpec,
    PieChartSpec,
)
from finance_query_agent.schemas.tool_results import (
    CategorySpending,
    MerchantSpending,
    RecurringExpense,
)
from finance_query_agent.visualization import (
    _chartable_row_count,
    _serialize_tool_results,
    generate_visualizations,
    should_visualize,
)

# ── should_visualize ─────────────────────────────────────────────────────────


_TWO_CATEGORIES = [
    CategorySpending(category="Food", total_amount=Decimal("100"), transaction_count=5, currency="USD"),
    CategorySpending(category="Transport", total_amount=Decimal("50"), transaction_count=3, currency="USD"),
]


class TestShouldVisualize:
    def test_returns_true_with_enough_rows(self):
        assert should_visualize([("get_spending_by_category", _TWO_CATEGORIES)]) is True

    def test_returns_true_for_each_chartable_tool(self):
        tools = (
            "get_category_breakdown",
            "get_monthly_totals",
            "get_spending_trend",
            "compare_periods",
            "get_top_merchants",
        )
        for tool in tools:
            assert should_visualize([(tool, ["a", "b"])]) is True, tool

    def test_returns_false_for_non_chartable_tools(self):
        assert should_visualize([("search_transactions", ["a", "b"])]) is False

    def test_returns_false_for_recurring_expenses(self):
        assert should_visualize([("get_recurring_expenses", ["a", "b"])]) is False

    def test_returns_false_for_balance_summary(self):
        assert should_visualize([("get_balance_summary", ["a", "b"])]) is False

    def test_returns_false_for_fallback_sql(self):
        assert should_visualize([("run_constrained_query", ["a", "b"])]) is False

    def test_returns_false_for_empty(self):
        assert should_visualize([]) is False

    def test_returns_false_for_single_row(self):
        assert should_visualize([("get_spending_by_category", [_TWO_CATEGORIES[0]])]) is False

    def test_returns_false_for_empty_chartable_data(self):
        assert should_visualize([("get_spending_by_category", [])]) is False

    def test_mixed_chartable_and_non_chartable(self):
        results = [
            ("search_transactions", []),
            ("get_spending_by_category", _TWO_CATEGORIES),
        ]
        assert should_visualize(results) is True

    def test_rows_accumulate_across_chartable_tools(self):
        merchant = MerchantSpending(
            merchant_name="X",
            total_amount=Decimal("10"),
            transaction_count=1,
            currency="USD",
        )
        results = [
            ("get_spending_by_category", [_TWO_CATEGORIES[0]]),
            ("get_top_merchants", [merchant]),
        ]
        assert should_visualize(results) is True


class TestChartableRowCount:
    def test_counts_list_items(self):
        assert _chartable_row_count([("get_spending_by_category", _TWO_CATEGORIES)]) == 2

    def test_counts_non_list_as_one(self):
        assert _chartable_row_count([("get_spending_by_category", "scalar")]) == 1

    def test_ignores_non_chartable(self):
        assert _chartable_row_count([("search_transactions", ["a", "b", "c"])]) == 0

    def test_sums_across_tools(self):
        results = [
            ("get_spending_by_category", ["a"]),
            ("get_top_merchants", ["b", "c"]),
        ]
        assert _chartable_row_count(results) == 3


# ── _serialize_tool_results ──────────────────────────────────────────────────


class TestSerializeToolResults:
    def test_serializes_pydantic_models(self):
        data = [
            CategorySpending(category="Food", total_amount=Decimal("100"), transaction_count=5, currency="USD"),
        ]
        result = _serialize_tool_results([("get_spending_by_category", data)])
        assert "get_spending_by_category" in result
        assert "Food" in result
        assert "100" in result

    def test_skips_non_chartable_tools(self):
        data = [
            RecurringExpense(
                merchant_name="Netflix",
                estimated_amount=Decimal("12.99"),
                frequency="monthly",
                occurrences=3,
                total_amount=Decimal("38.97"),
                currency="USD",
            ),
        ]
        result = _serialize_tool_results([("get_recurring_expenses", data)])
        assert result == ""

    def test_handles_multiple_tools(self):
        spending = [
            CategorySpending(category="Food", total_amount=Decimal("100"), transaction_count=5, currency="USD"),
        ]
        merchants = [
            MerchantSpending(
                merchant_name="Whole Foods", total_amount=Decimal("80"), transaction_count=3, currency="USD"
            ),
        ]
        result = _serialize_tool_results(
            [
                ("get_spending_by_category", spending),
                ("get_top_merchants", merchants),
            ]
        )
        assert "get_spending_by_category" in result
        assert "get_top_merchants" in result

    def test_empty_data_still_serializes(self):
        result = _serialize_tool_results([("get_spending_by_category", [])])
        assert "get_spending_by_category" in result
        assert "[]" in result


# ── Chart spec model validation ──────────────────────────────────────────────


class TestChartSpecModels:
    def test_pie_chart_spec(self):
        chart = PieChartSpec(
            title="Spending by Category (USD)",
            currency="USD",
            slices=[
                {"label": "Food", "value": 150.0, "percentage": 60.0},
                {"label": "Transport", "value": 100.0, "percentage": 40.0},
            ],
        )
        assert chart.chart_type == "pie"
        assert len(chart.slices) == 2
        dumped = chart.model_dump()
        assert dumped["chart_type"] == "pie"

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
        assert chart.series_labels == ("Oct 2025", "Nov 2025")


# ── AgentResponse with visualizations ────────────────────────────────────────


class TestAgentResponseVisualization:
    def test_response_without_visualizations(self):
        from finance_query_agent.schemas.responses import AgentResponse, TokenUsage

        resp = AgentResponse(
            answer="test",
            tool_calls=[],
            fallback_used=False,
            fallback_sql=None,
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
            slices=[{"label": "A", "value": 100.0, "percentage": 100.0}],
        )
        resp = AgentResponse(
            answer="test",
            tool_calls=[],
            visualizations=[chart],
            fallback_used=False,
            fallback_sql=None,
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
            fallback_used=False,
            fallback_sql=None,
            unresolved=False,
            original_question="test",
            token_usage=TokenUsage(input_tokens=0, output_tokens=0),
        )
        json_str = resp.model_dump_json()
        restored = AgentResponse.model_validate_json(json_str)
        assert len(restored.visualizations) == 1
        assert restored.visualizations[0].chart_type == "bar"


# ── generate_visualizations edge cases ───────────────────────────────────────


class TestGenerateVisualizations:
    def test_returns_none_for_non_chartable(self):
        result = asyncio.run(
            generate_visualizations("query", [("search_transactions", ["a", "b"])]),
        )
        assert result is None

    def test_returns_none_for_single_row(self):
        data = [CategorySpending(category="Food", total_amount=Decimal("100"), transaction_count=5, currency="USD")]
        result = asyncio.run(
            generate_visualizations("query", [("get_spending_by_category", data)]),
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
                        generate_visualizations("spending?", [("get_spending_by_category", _TWO_CATEGORIES)]),
                        timeout=0.1,
                    )
                except TimeoutError:
                    return None

            result = asyncio.run(_run())
            assert result is None
