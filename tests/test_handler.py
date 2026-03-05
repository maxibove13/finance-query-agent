"""Tests for Lambda handler — direct invocation envelope and _process_request orchestration."""

from __future__ import annotations

import asyncio
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic_ai.exceptions import UsageLimitExceeded

import finance_query_agent.handler as handler_module
from finance_query_agent.handler import _process_request, handler
from finance_query_agent.validation.schema_validator import ColumnTypeInfo

_PATCH_TARGET = "finance_query_agent.handler._process_request"

_EVENT = {"user_id": 1, "session_id": "s1", "question": "q"}


# ── Outer handler (direct invocation envelope) ──────────────────────────────


class TestHandler:
    def test_returns_response_on_success(self) -> None:
        mock_response = MagicMock()
        mock_response.model_dump.return_value = {"answer": "test"}

        with patch(_PATCH_TARGET, new_callable=AsyncMock, return_value=mock_response):
            result = handler(_EVENT, None)

        assert result["answer"] == "test"

    def test_returns_error_on_missing_field(self) -> None:
        with patch(_PATCH_TARGET, new_callable=AsyncMock, side_effect=KeyError("user_id")):
            result = handler({"session_id": "s1"}, None)

        assert "user_id" in result["error"]

    def test_returns_error_on_schema_mismatch(self) -> None:
        from finance_query_agent.exceptions import SchemaValidationError

        with patch(
            _PATCH_TARGET,
            new_callable=AsyncMock,
            side_effect=SchemaValidationError("column 'foo' does not exist on table 'bar'"),
        ):
            result = handler(_EVENT, None)

        assert result["error"] == "schema_mismatch"
        assert "foo" in result["message"]

    def test_returns_error_on_unexpected_error(self) -> None:
        with patch(_PATCH_TARGET, new_callable=AsyncMock, side_effect=RuntimeError("boom")):
            result = handler(_EVENT, None)

        assert "error" in result


# ── _process_request orchestration ──────────────────────────────────────────

_BODY = {"user_id": 1, "session_id": "s1", "question": "test?"}


def _build_mocks() -> dict:
    """Build mock objects for all _process_request external dependencies."""
    conn = AsyncMock()
    memory = AsyncMock()
    memory.load_history.return_value = []

    usage = MagicMock(input_tokens=100, output_tokens=50)
    result = MagicMock()
    result.output = "The answer is 42"
    result.all_messages.return_value = [{"role": "user", "content": "q"}]
    result.usage.return_value = usage

    agent = MagicMock()
    agent.run = AsyncMock(return_value=result)

    settings = MagicMock()
    settings.database_url = "postgresql://test@localhost/db"
    settings.encryption_key = None
    settings.dynamodb_table = "t"
    settings.dynamodb_region = "us-east-1"
    settings.llm_model = "test:m"
    settings.agent_request_limit = 7
    settings.agent_per_request_timeout = 12.0
    settings.agent_run_timeout = 25.0

    targets = {
        "finance_query_agent.observability.initialize": MagicMock(),
        "finance_query_agent.config.get_settings": MagicMock(return_value=settings),
        "finance_query_agent.config.load_schema_json": MagicMock(return_value={}),
        "finance_query_agent.schemas.mapping.SchemaMapping": MagicMock(),
        "finance_query_agent.connection.Connection": MagicMock(return_value=conn),
        "finance_query_agent.encryption.FieldEncryptor": MagicMock(),
        "finance_query_agent.memory.ConversationMemory": MagicMock(return_value=memory),
        "finance_query_agent.query_builder.QueryBuilder": MagicMock(),
        "finance_query_agent.validation.schema_validator.validate_schema": AsyncMock(
            return_value=ColumnTypeInfo(user_id_type="int4", direction_is_enum=True)
        ),
        "finance_query_agent.agent.get_agent": MagicMock(return_value=agent),
    }

    return {"conn": conn, "memory": memory, "agent": agent, "settings": settings, "targets": targets}


def _apply(stack: ExitStack, targets: dict) -> None:
    for target, mock_obj in targets.items():
        stack.enter_context(patch(target, mock_obj))


class TestProcessRequest:
    """Tests for _process_request orchestration (mocked external deps)."""

    @pytest.fixture(autouse=True)
    def _reset_init(self):
        handler_module._initialized = False
        yield
        handler_module._initialized = False

    @pytest.fixture()
    def mocks(self):
        return _build_mocks()

    @pytest.mark.asyncio()
    async def test_returns_correct_response(self, mocks: dict) -> None:
        with ExitStack() as stack:
            _apply(stack, mocks["targets"])
            resp = await _process_request(_BODY)

        assert resp.answer == "The answer is 42"
        assert resp.original_question == "test?"
        assert resp.token_usage.input_tokens == 100
        assert resp.token_usage.output_tokens == 50
        assert resp.unresolved is True  # no tool_calls, no fallback

    @pytest.mark.asyncio()
    async def test_closes_connection_on_success(self, mocks: dict) -> None:
        with ExitStack() as stack:
            _apply(stack, mocks["targets"])
            await _process_request(_BODY)

        mocks["conn"].close.assert_awaited_once()

    @pytest.mark.asyncio()
    async def test_closes_connection_on_agent_error(self, mocks: dict) -> None:
        mocks["agent"].run = AsyncMock(side_effect=RuntimeError("LLM down"))

        with ExitStack() as stack:
            _apply(stack, mocks["targets"])
            with pytest.raises(RuntimeError, match="LLM down"):
                await _process_request(_BODY)

        mocks["conn"].close.assert_awaited_once()

    @pytest.mark.asyncio()
    async def test_missing_body_field_raises_key_error(self, mocks: dict) -> None:
        with ExitStack() as stack:
            _apply(stack, mocks["targets"])
            with pytest.raises(KeyError):
                await _process_request({"session_id": "s1", "question": "q"})

    @pytest.mark.asyncio()
    async def test_initialize_called_once_across_requests(self, mocks: dict) -> None:
        init_mock = mocks["targets"]["finance_query_agent.observability.initialize"]

        with ExitStack() as stack:
            _apply(stack, mocks["targets"])
            await _process_request(_BODY)
            await _process_request(_BODY)

        init_mock.assert_called_once()

    @pytest.mark.asyncio()
    async def test_saves_history_after_run(self, mocks: dict) -> None:
        with ExitStack() as stack:
            _apply(stack, mocks["targets"])
            await _process_request(_BODY)

        mocks["memory"].save_history.assert_awaited_once()
        args = mocks["memory"].save_history.call_args[0]
        assert args[0] == "1"  # DynamoDB uses string keys
        assert args[1] == "s1"

    @pytest.mark.asyncio()
    async def test_passes_user_id_to_agent_deps(self, mocks: dict) -> None:
        with ExitStack() as stack:
            _apply(stack, mocks["targets"])
            await _process_request(_BODY)

        deps = mocks["agent"].run.call_args.kwargs["deps"]
        assert deps.user_id == 1

    @pytest.mark.asyncio()
    async def test_returns_200_on_usage_limit_exceeded(self, mocks: dict) -> None:
        mocks["agent"].run = AsyncMock(side_effect=UsageLimitExceeded("request_limit of 7 exceeded"))

        with ExitStack() as stack:
            _apply(stack, mocks["targets"])
            resp = await _process_request(_BODY)

        assert resp.unresolved is True
        assert "time limit" in resp.answer
        assert resp.original_question == "test?"

    @pytest.mark.asyncio()
    async def test_returns_200_on_timeout(self, mocks: dict) -> None:
        mocks["settings"].agent_run_timeout = 0.01

        async def _hang(*args, **kwargs):
            await asyncio.sleep(999)

        mocks["agent"].run = AsyncMock(side_effect=_hang)

        with ExitStack() as stack:
            _apply(stack, mocks["targets"])
            resp = await _process_request(_BODY)

        assert resp.unresolved is True
        assert "time limit" in resp.answer

    @pytest.mark.asyncio()
    async def test_closes_connection_on_timeout(self, mocks: dict) -> None:
        mocks["agent"].run = AsyncMock(side_effect=UsageLimitExceeded("exceeded"))

        with ExitStack() as stack:
            _apply(stack, mocks["targets"])
            await _process_request(_BODY)

        mocks["conn"].close.assert_awaited_once()

    @pytest.mark.asyncio()
    async def test_does_not_save_history_on_timeout(self, mocks: dict) -> None:
        mocks["agent"].run = AsyncMock(side_effect=UsageLimitExceeded("exceeded"))

        with ExitStack() as stack:
            _apply(stack, mocks["targets"])
            await _process_request(_BODY)

        mocks["memory"].save_history.assert_not_awaited()

    @pytest.mark.asyncio()
    async def test_passes_usage_limits_to_agent_run(self, mocks: dict) -> None:
        with ExitStack() as stack:
            _apply(stack, mocks["targets"])
            await _process_request(_BODY)

        kwargs = mocks["agent"].run.call_args.kwargs
        assert kwargs["usage_limits"].request_limit == 7

    @pytest.mark.asyncio()
    async def test_passes_model_settings_to_agent_run(self, mocks: dict) -> None:
        with ExitStack() as stack:
            _apply(stack, mocks["targets"])
            await _process_request(_BODY)

        kwargs = mocks["agent"].run.call_args.kwargs
        assert kwargs["model_settings"]["timeout"] == 12.0
