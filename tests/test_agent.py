"""Tests for agent.py — agent factory and system prompt."""

from __future__ import annotations

import datetime

from pydantic_ai.models.test import TestModel

import finance_query_agent.agent as agent_module
from finance_query_agent.agent import build_system_prompt, get_agent

_TEST_MODEL = TestModel()

_FAKE_SCHEMA = (
    "tables:\n  account_movements:\n  accounts:\n  credit_card_movements:\n"
    "columns:\n  movement_direction: text [values: DEBIT, CREDIT]"
)


class TestBuildSystemPrompt:
    def test_includes_current_date(self) -> None:
        prompt = build_system_prompt(_FAKE_SCHEMA)
        assert datetime.date.today().isoformat() in prompt

    def test_includes_schema_tables(self) -> None:
        prompt = build_system_prompt(_FAKE_SCHEMA)
        assert "account_movements" in prompt
        assert "accounts" in prompt
        assert "credit_card_movements" in prompt
        assert "movement_direction" in prompt

    def test_includes_amount_convention(self) -> None:
        prompt = build_system_prompt(_FAKE_SCHEMA)
        assert "DEBIT" in prompt
        assert "CREDIT" in prompt

    def test_includes_render_tool_guidance(self) -> None:
        prompt = build_system_prompt(_FAKE_SCHEMA)
        assert "render_donut_chart" in prompt
        assert "render_metric_card" in prompt
        assert "render tool" in prompt

    def test_includes_language_mirroring(self) -> None:
        prompt = build_system_prompt(_FAKE_SCHEMA)
        assert "same language" in prompt

    def test_no_old_tool_references(self) -> None:
        prompt = build_system_prompt(_FAKE_SCHEMA)
        for deleted in (
            "query_expenses",
            "query_income",
            "query_balance_history",
            "search_transactions",
            "get_recurring_expenses",
        ):
            assert deleted not in prompt, f"Old tool '{deleted}' still referenced in system prompt"


class TestGetAgent:
    def setup_method(self) -> None:
        agent_module._agents.clear()

    def teardown_method(self) -> None:
        agent_module._agents.clear()

    def test_returns_agent_with_all_tools(self) -> None:
        agent = get_agent(_TEST_MODEL)
        tool_names = set(agent._function_toolset.tools.keys())
        assert "execute_sql" in tool_names
        assert "render_donut_chart" in tool_names
        assert "render_metric_card" in tool_names
        assert "render_cash_flow" in tool_names

    def test_singleton_behavior(self) -> None:
        a1 = get_agent(_TEST_MODEL)
        a2 = get_agent(_TEST_MODEL)
        assert a1 is a2

    def test_has_structured_output(self) -> None:
        agent = get_agent(_TEST_MODEL)
        output_tool_names = {t.name for t in agent._output_toolset._tool_defs}
        assert "final_result" in output_tool_names

    def test_retries_set_to_three(self) -> None:
        agent = get_agent(_TEST_MODEL)
        assert agent._max_result_retries == 3
