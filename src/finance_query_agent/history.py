"""LLM-based conversation history summarization."""

from __future__ import annotations

from pydantic_ai import Agent
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    ToolCallPart,
    ToolReturnPart,
)

KEEP_RECENT = 6
SUMMARIZE_THRESHOLD = 20

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
        _summarizer = Agent("openai:gpt-4o-mini", instructions=_SUMMARIZER_INSTRUCTIONS)
    return _summarizer


def _is_tool_message(msg: ModelMessage) -> bool:
    """Check if a message contains tool call or tool return parts."""
    if isinstance(msg, ModelRequest):
        return any(isinstance(p, ToolReturnPart) for p in msg.parts)
    if isinstance(msg, ModelResponse):
        return any(isinstance(p, ToolCallPart) for p in msg.parts)
    return False


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

    summarizer = _get_summarizer()
    result = await summarizer.run(
        "Summarize the conversation above.",
        message_history=to_summarize,
    )
    return result.new_messages() + to_keep
