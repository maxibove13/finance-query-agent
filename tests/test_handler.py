"""Tests for Lambda handler — HTTP envelope and _process_request orchestration."""

from __future__ import annotations

import json
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import finance_query_agent.handler as handler_module
from finance_query_agent.handler import _process_request, handler

_PATCH_TARGET = "finance_query_agent.handler._process_request"


# ── Outer handler (HTTP envelope) ───────────────────────────────────────────


class TestHandler:
    def test_returns_200_on_success(self) -> None:
        mock_response = MagicMock()
        mock_response.model_dump_json.return_value = json.dumps({"answer": "test"})

        with patch(_PATCH_TARGET, new_callable=AsyncMock, return_value=mock_response):
            result = handler(
                {"body": json.dumps({"user_id": "u1", "session_id": "s1", "question": "q"})},
                None,
            )

        assert result["statusCode"] == 200
        assert result["headers"]["Content-Type"] == "application/json"
        body = json.loads(result["body"])
        assert body["answer"] == "test"

    def test_returns_400_on_missing_field(self) -> None:
        with patch(_PATCH_TARGET, new_callable=AsyncMock, side_effect=KeyError("user_id")):
            result = handler({"body": "{}"}, None)

        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "user_id" in body["error"]

    def test_returns_500_on_unexpected_error(self) -> None:
        with patch(_PATCH_TARGET, new_callable=AsyncMock, side_effect=RuntimeError("boom")):
            result = handler(
                {"body": json.dumps({"user_id": "u1", "session_id": "s1", "question": "q"})},
                None,
            )

        assert result["statusCode"] == 500
        body = json.loads(result["body"])
        assert "error" in body


# ── _process_request orchestration ──────────────────────────────────────────

_BODY = {"user_id": "u1", "session_id": "s1", "question": "test?"}


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

    targets = {
        "finance_query_agent.observability.initialize": MagicMock(),
        "finance_query_agent.config.get_settings": MagicMock(return_value=settings),
        "finance_query_agent.config.load_schema_json": MagicMock(return_value={}),
        "finance_query_agent.schemas.mapping.SchemaMapping": MagicMock(),
        "finance_query_agent.connection.Connection": MagicMock(return_value=conn),
        "finance_query_agent.encryption.FieldEncryptor": MagicMock(),
        "finance_query_agent.memory.ConversationMemory": MagicMock(return_value=memory),
        "finance_query_agent.query_builder.QueryBuilder": MagicMock(),
        "finance_query_agent.validation.schema_validator.validate_schema": AsyncMock(),
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
        assert args[0] == "u1"
        assert args[1] == "s1"

    @pytest.mark.asyncio()
    async def test_passes_user_id_to_agent_deps(self, mocks: dict) -> None:
        with ExitStack() as stack:
            _apply(stack, mocks["targets"])
            await _process_request(_BODY)

        deps = mocks["agent"].run.call_args.kwargs["deps"]
        assert deps.user_id == "u1"
