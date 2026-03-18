"""LLM-based conversation history summarization."""

from __future__ import annotations

from pydantic_ai import Agent
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
)

from finance_query_agent.config import get_settings

KEEP_RECENT = 6
SUMMARIZE_THRESHOLD = 20
SUMMARY_MARKER = "[PRIOR CONTEXT SUMMARIZED]"

_SUMMARIZER_INSTRUCTIONS = (
    "Summarize this financial assistant conversation concisely. "
    "Preserve: questions asked, time periods mentioned, key totals, categories discussed. "
    "Omit: verbose tool output rows, repeated clarifications. "
    "Output: 3-5 sentences maximum."
)

_summarizer: Agent[None, str] | None = None


def _get_summarizer() -> Agent[None, str]:
    global _summarizer
    if _summarizer is None:
        _summarizer = Agent(get_settings().secondary_model, instructions=_SUMMARIZER_INSTRUCTIONS)
    return _summarizer


def _is_tool_message(msg: ModelMessage) -> bool:
    """Check if a message contains tool call or tool return parts."""
    if isinstance(msg, ModelRequest):
        return any(isinstance(p, ToolReturnPart) for p in msg.parts)
    if isinstance(msg, ModelResponse):
        return any(isinstance(p, ToolCallPart) for p in msg.parts)
    return False


def _is_summary_message(msg: ModelMessage) -> bool:
    """True if msg is a synthetic summary produced by a prior summarize_history call."""
    return isinstance(msg, ModelResponse) and any(
        isinstance(p, TextPart) and p.content.startswith(SUMMARY_MARKER) for p in msg.parts
    )


async def summarize_history(messages: list[ModelMessage]) -> list[ModelMessage]:
    """Compress old messages via LLM summarization. Keeps recent messages verbatim."""
    if len(messages) <= SUMMARIZE_THRESHOLD:
        return messages

    to_summarize = messages[:-KEEP_RECENT]
    to_keep = messages[-KEEP_RECENT:]

    # Walk backward to avoid cutting inside tool call/response pairs
    while to_summarize and _is_tool_message(to_summarize[-1]):
        to_keep.insert(0, to_summarize.pop())

    if not to_summarize:
        return messages  # can't safely split

    # Strip a prior synthetic summary from the front to avoid summarizing a summary.
    # A fresh summary of the real messages that followed is more accurate.
    if _is_summary_message(to_summarize[0]):
        to_summarize = to_summarize[1:]

    if not to_summarize:
        return messages  # only a prior summary remained; nothing new to compress

    summarizer = _get_summarizer()
    result = await summarizer.run(
        "Summarize the conversation above.",
        message_history=to_summarize,
    )
    summary = ModelResponse(parts=[TextPart(content=f"{SUMMARY_MARKER}\n{result.output}")])
    return [summary] + to_keep
