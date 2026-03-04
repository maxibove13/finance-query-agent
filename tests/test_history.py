"""Tests for conversation history summarization."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic_ai.messages import (
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)

from finance_query_agent.history import (
    KEEP_RECENT,
    SUMMARIZE_THRESHOLD,
    _is_tool_message,
    summarize_history,
)


def _make_user_msg(text: str) -> ModelRequest:
    return ModelRequest(parts=[UserPromptPart(content=text)])


def _make_assistant_msg(text: str) -> ModelResponse:
    return ModelResponse(parts=[TextPart(content=text)])


def _make_tool_call_msg() -> ModelResponse:
    return ModelResponse(parts=[ToolCallPart(tool_name="test_tool", args={"a": 1}, tool_call_id="tc1")])


def _make_tool_return_msg() -> ModelRequest:
    return ModelRequest(parts=[ToolReturnPart(tool_name="test_tool", content="result", tool_call_id="tc1")])


def _mock_summarizer():
    """Create a mock summarizer agent that returns summary messages."""
    summary_messages = [
        _make_user_msg("Summarize the conversation above."),
        _make_assistant_msg("Summary of the conversation."),
    ]
    mock_result = MagicMock()
    mock_result.new_messages.return_value = summary_messages
    mock_agent = MagicMock()
    mock_agent.run = AsyncMock(return_value=mock_result)
    return mock_agent


class TestIsToolMessage:
    def test_tool_return_is_tool_message(self):
        assert _is_tool_message(_make_tool_return_msg()) is True

    def test_tool_call_is_tool_message(self):
        assert _is_tool_message(_make_tool_call_msg()) is True

    def test_user_message_is_not_tool_message(self):
        assert _is_tool_message(_make_user_msg("hello")) is False

    def test_text_response_is_not_tool_message(self):
        assert _is_tool_message(_make_assistant_msg("hi")) is False


class TestSummarizeHistory:
    @pytest.mark.anyio
    async def test_below_threshold_returns_unchanged(self):
        messages = [_make_user_msg(f"msg {i}") for i in range(SUMMARIZE_THRESHOLD - 1)]
        result = await summarize_history(messages)
        assert result is messages

    @pytest.mark.anyio
    async def test_at_threshold_returns_unchanged(self):
        messages = [_make_user_msg(f"msg {i}") for i in range(SUMMARIZE_THRESHOLD)]
        result = await summarize_history(messages)
        assert result is messages

    @pytest.mark.anyio
    async def test_above_threshold_triggers_summarization(self):
        messages = [_make_user_msg(f"msg {i}") for i in range(SUMMARIZE_THRESHOLD + 5)]

        mock_agent = _mock_summarizer()
        with patch("finance_query_agent.history._get_summarizer", return_value=mock_agent):
            result = await summarize_history(messages)

        # 2 summary messages + KEEP_RECENT kept
        assert len(result) == 2 + KEEP_RECENT
        mock_agent.run.assert_called_once()

    @pytest.mark.anyio
    async def test_respects_tool_call_pairs(self):
        """Tool messages at the split boundary should be moved to the 'keep' side."""
        normal_msgs = [_make_user_msg(f"msg {i}") for i in range(SUMMARIZE_THRESHOLD)]
        tool_msgs = [_make_tool_call_msg(), _make_tool_return_msg()]
        tail_msgs = [_make_user_msg(f"tail {i}") for i in range(KEEP_RECENT - 2)]
        messages = normal_msgs + tool_msgs + tail_msgs

        assert len(messages) > SUMMARIZE_THRESHOLD

        mock_agent = _mock_summarizer()
        with patch("finance_query_agent.history._get_summarizer", return_value=mock_agent):
            result = await summarize_history(messages)

        assert len(result) >= KEEP_RECENT

    @pytest.mark.anyio
    async def test_all_tool_messages_returns_unchanged(self):
        """If all summarizable messages are tool messages, return unchanged."""
        tool_msgs = []
        for _ in range(SUMMARIZE_THRESHOLD + 1 - KEEP_RECENT):
            tool_msgs.extend([_make_tool_call_msg(), _make_tool_return_msg()])
        tail = [_make_user_msg(f"tail {i}") for i in range(KEEP_RECENT)]
        messages = tool_msgs + tail

        if len(messages) <= SUMMARIZE_THRESHOLD:
            messages = tool_msgs + tool_msgs + tail

        result = await summarize_history(messages)
        assert result is messages
