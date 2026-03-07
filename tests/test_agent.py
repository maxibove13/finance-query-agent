"""Tests for agent.py — agent factory and system prompt."""

from __future__ import annotations

import datetime

from pydantic_ai.models.test import TestModel

import finance_query_agent.agent as agent_module
from finance_query_agent.agent import build_system_prompt, get_agent

_TEST_MODEL = TestModel()


class TestBuildSystemPrompt:
    def test_includes_current_date(self) -> None:
        prompt = build_system_prompt()
        assert datetime.date.today().isoformat() in prompt

    def test_includes_tool_selection_guidance(self) -> None:
        prompt = build_system_prompt()
        assert "run_constrained_query" in prompt
        assert "query_expenses" in prompt
        assert "query_income" in prompt
        assert "query_balance_history" in prompt
        assert "search_transactions" in prompt
        assert "get_recurring_expenses" in prompt

    def test_includes_visualization_guidance(self) -> None:
        prompt = build_system_prompt()
        assert "final_answer_with_chart" in prompt
        assert "final_answer" in prompt
        assert "visualization agent" in prompt

    def test_no_deleted_tool_references(self) -> None:
        prompt = build_system_prompt()
        for deleted in (
            "get_spending_by_category",
            "get_monthly_totals",
            "get_balance_summary",
            "get_top_merchants",
            "compare_periods",
            "get_spending_trend",
            "get_category_breakdown",
        ):
            assert deleted not in prompt, f"Deleted tool '{deleted}' still referenced in system prompt"


class TestGetAgent:
    def setup_method(self) -> None:
        agent_module._agents.clear()

    def teardown_method(self) -> None:
        agent_module._agents.clear()

    def test_returns_agent_with_all_tools(self) -> None:
        agent = get_agent(_TEST_MODEL)
        tool_names = set(agent._function_toolset.tools.keys())
        assert tool_names == {
            "search_transactions",
            "get_recurring_expenses",
            "run_constrained_query",
            "query_expenses",
            "query_income",
            "query_balance_history",
        }

    def test_singleton_behavior(self) -> None:
        a1 = get_agent(_TEST_MODEL)
        a2 = get_agent(_TEST_MODEL)
        assert a1 is a2

    def test_has_output_tools(self) -> None:
        agent = get_agent(_TEST_MODEL)
        output_tool_names = {t.name for t in agent._output_toolset._tool_defs}
        assert output_tool_names == {"final_answer", "final_answer_with_chart"}

    def test_retries_set_to_three(self) -> None:
        agent = get_agent(_TEST_MODEL)
        assert agent._max_result_retries == 3
