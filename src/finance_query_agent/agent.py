"""Pydantic AI agent definition, system prompt, tool registration."""

from __future__ import annotations

import datetime

from pydantic_ai import Agent, RunContext, ToolOutput
from pydantic_ai.models import Model

from finance_query_agent.history import summarize_history
from finance_query_agent.schemas.responses import AgentOutput, AnswerWithVisualization, TextAnswer
from finance_query_agent.tools import AgentDeps

_agents: dict[str, Agent[AgentDeps, AgentOutput]] = {}


def get_agent(model: str | Model) -> Agent[AgentDeps, AgentOutput]:
    """Cached agent factory, keyed by model. Reused across warm Lambda invocations."""
    key = str(model)
    if key in _agents:
        return _agents[key]

    # Import tools here to avoid circular imports at module level
    from pydantic_ai import Tool

    from finance_query_agent.tools.fallback_sql import run_constrained_query
    from finance_query_agent.tools.recurring import get_recurring_expenses
    from finance_query_agent.tools.spending import (
        _prepare_balance_summary,
        get_balance_summary,
        get_monthly_totals,
        get_spending_by_category,
    )
    from finance_query_agent.tools.transactions import get_top_merchants, search_transactions
    from finance_query_agent.tools.trends import compare_periods, get_category_breakdown, get_spending_trend

    agent: Agent[AgentDeps, AgentOutput] = Agent(
        model,
        deps_type=AgentDeps,
        output_type=[
            ToolOutput(
                TextAnswer,
                name="final_answer",
                description="Return a text-only answer.",
            ),
            ToolOutput(
                AnswerWithVisualization,
                name="final_answer_with_chart",
                description=(
                    "Return a text answer and trigger chart generation from the tool results. "
                    "Use when the data is categorical, comparative, or time-series and a chart would add value."
                ),
            ),
        ],
        tools=[
            get_spending_by_category,
            get_monthly_totals,
            Tool(get_balance_summary, prepare=_prepare_balance_summary),  # type: ignore[arg-type]
            get_top_merchants,
            search_transactions,
            compare_periods,
            get_spending_trend,
            get_category_breakdown,
            get_recurring_expenses,
            run_constrained_query,
        ],
        retries=3,
        history_processors=[summarize_history],
    )

    @agent.system_prompt(dynamic=True)
    async def system_prompt(ctx: RunContext[AgentDeps]) -> str:
        return build_system_prompt()

    _agents[key] = agent
    return agent


def build_system_prompt() -> str:
    """Build system prompt with fresh date. Called on every agent.run()."""
    today = datetime.date.today().isoformat()
    return f"""You are a financial data assistant. Today's date is {today}.

Your job is to answer questions about the user's financial transactions using the available tools.

Guidelines:
- Resolve relative dates to absolute dates before calling tools.
  "last month" means the previous calendar month relative to today.
- Prefer predefined tools (get_spending_by_category, get_monthly_totals, etc.) over the SQL fallback tool.
- Only use run_constrained_query when no predefined tool can answer the question.
- If a tool returns empty results, say so honestly. Never fabricate data.
- Format monetary values with currency codes and two decimal places (e.g., 1,234.56 USD).
- When results span multiple currencies, present each currency separately. Never convert or sum across currencies.
- If the user's question is ambiguous, ask a clarifying question rather than guessing.
- Keep responses concise and focused on the data.
- When your tool results contain data that would benefit from a visual chart (categorical breakdowns,
  time-series trends, period comparisons), use the final_answer_with_chart output.
  A separate visualization agent will create structured chart specs from the tool results.
  Do not format charts, tables, or visual data in your text — provide a clear text summary only.
- Use final_answer when the data is not chartable, when results are empty, or for simple factual answers."""
