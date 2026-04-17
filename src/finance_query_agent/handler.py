"""Lambda handler. Synchronous HTTP request-response."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

from finance_query_agent.redaction import sanitize_error

if TYPE_CHECKING:
    from finance_query_agent.schemas.responses import AgentResponse

logger = logging.getLogger(__name__)

_initialized: bool = False
_loop: asyncio.AbstractEventLoop | None = None


def _get_event_loop() -> asyncio.AbstractEventLoop:
    """Return a persistent event loop, creating one if needed.

    Unlike asyncio.run() which creates and closes a loop per call,
    this keeps the loop alive across warm Lambda invocations so cached
    async resources (connection pools, HTTP clients) remain valid.
    """
    global _loop  # noqa: PLW0603
    if _loop is None or _loop.is_closed():
        _loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_loop)
    return _loop


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Lambda handler. Invoked directly via boto3 lambda.invoke()."""
    try:
        loop = _get_event_loop()
        result = loop.run_until_complete(_process_request(event))
        return result.model_dump()
    except KeyError as e:
        logger.warning("Missing required field: %s", e)
        return {"error": f"Missing required field: {e}"}
    except ValueError as e:
        logger.warning("Invalid input: %s", e)
        return {"error": f"Invalid input: {e}"}
    except Exception as e:
        logger.exception("Agent request failed")
        return {"error": sanitize_error(e)}


async def _process_request(body: dict[str, Any]) -> AgentResponse:
    """Orchestrate agent execution: connect, load history, run, save history."""
    global _initialized  # noqa: PLW0603

    from finance_query_agent.agent import get_agent
    from finance_query_agent.config import get_settings
    from finance_query_agent.connection import Connection
    from finance_query_agent.encryption import FieldEncryptor
    from finance_query_agent.memory import ConversationMemory
    from finance_query_agent.schemas.responses import AgentResponse, TokenUsage
    from finance_query_agent.tools import AgentDeps

    request_start = time.monotonic()

    raw_user_id = body["user_id"]
    session_id = body["session_id"]
    question = body["question"]

    settings = get_settings()

    # --- Input validation ---
    if not isinstance(question, str) or not question.strip():
        raise ValueError("question must be a non-empty string")
    question = question.strip()
    if len(question) > settings.max_question_length:
        raise ValueError(f"question exceeds maximum length of {settings.max_question_length} characters")
    if not isinstance(session_id, str) or not session_id.strip():
        raise ValueError("session_id must be a non-empty string")
    session_id = session_id.strip()
    if len(session_id) > settings.max_session_id_length:
        raise ValueError(f"session_id exceeds maximum length of {settings.max_session_id_length} characters")

    # user_id must be a positive integer (int or numeric string)
    if isinstance(raw_user_id, bool):
        raise ValueError("user_id must be an integer or string, got bool")
    if isinstance(raw_user_id, int):
        int_user_id: int = raw_user_id
        if int_user_id <= 0:
            raise ValueError(f"user_id must be a positive integer, got {int_user_id}")
        user_id: int | str = int_user_id
    elif isinstance(raw_user_id, str) and raw_user_id.isdigit():
        user_id = int(raw_user_id)
        if user_id <= 0:
            raise ValueError(f"user_id must be a positive integer, got {user_id}")
    else:
        raise ValueError(f"user_id must be a positive integer, got {raw_user_id!r}")

    encryptor = FieldEncryptor(settings.encryption_key)
    memory = ConversationMemory(settings.dynamodb_table, settings.dynamodb_region, encryptor)
    assert settings.database_url is not None, "database_url must be set"
    conn = Connection(settings.database_url)

    try:
        await conn.connect()

        if not _initialized:
            from finance_query_agent.observability import initialize

            initialize()
            # tags is intentionally excluded: it is shared reference data (global taxonomy),
            # not tenant-scoped. All user-owned tables are listed here.
            await conn.verify_rls_enabled(
                ["accounts", "credit_cards", "account_movements", "credit_card_movements"],
                strict=settings.aws_lambda_function_name is not None,
            )
            _initialized = True

        # Load conversation history (DynamoDB always uses string keys; use normalized int)
        history, history_version = await memory.load_history(str(user_id), session_id)

        # Run agent
        from pydantic_ai import UsageLimits
        from pydantic_ai.exceptions import UsageLimitExceeded
        from pydantic_ai.settings import ModelSettings

        deps = AgentDeps(connection=conn, user_id=user_id)
        agent = get_agent(settings.primary_model)

        usage_limits = UsageLimits(request_limit=settings.agent_request_limit)
        model_settings = ModelSettings(timeout=settings.agent_per_request_timeout)

        try:
            result = await asyncio.wait_for(
                agent.run(
                    question,
                    deps=deps,
                    message_history=history,
                    usage_limits=usage_limits,
                    model_settings=model_settings,
                ),
                timeout=settings.agent_run_timeout,
            )
        except (TimeoutError, UsageLimitExceeded) as exc:
            logger.warning("Agent execution capped: %s", exc)
            if settings.audit_table:
                try:
                    from finance_query_agent.audit import SqlAudit

                    audit = SqlAudit(settings.audit_table, settings.dynamodb_region)
                    await audit.write_invocation(
                        user_id=str(user_id),
                        session_id=session_id,
                        question=question,
                        tool_calls=deps.tool_calls,
                        token_usage=TokenUsage(input_tokens=0, output_tokens=0),
                        total_ms=int((time.monotonic() - request_start) * 1000),
                        unresolved=True,
                    )
                except Exception:
                    logger.warning("Audit write failed", exc_info=True)
            return AgentResponse(
                answer=(
                    "I wasn't able to fully process your question within the time limit."
                    " Please try rephrasing it or breaking it into simpler parts."
                ),
                tool_calls=deps.tool_calls,
                render_calls=deps.render_calls,
                unresolved=True,
                original_question=question,
                token_usage=TokenUsage(input_tokens=0, output_tokens=0),
            )

        # Store updated conversation (DynamoDB always uses string keys; use normalized int)
        from finance_query_agent.exceptions import ConversationConflictError

        try:
            await memory.save_history(str(user_id), session_id, result.all_messages(), history_version)
        except ConversationConflictError:
            logger.warning("Conversation conflict | user=%s session=%s", user_id, session_id)
            raise

        usage = result.usage()

        if settings.audit_table:
            try:
                from finance_query_agent.audit import SqlAudit

                audit = SqlAudit(settings.audit_table, settings.dynamodb_region)
                await audit.write_invocation(
                    user_id=str(user_id),
                    session_id=session_id,
                    question=question,
                    tool_calls=deps.tool_calls,
                    token_usage=TokenUsage(
                        input_tokens=usage.input_tokens or 0,
                        output_tokens=usage.output_tokens or 0,
                    ),
                    total_ms=int((time.monotonic() - request_start) * 1000),
                    unresolved=not deps.tool_calls,
                )
            except Exception:
                logger.warning("Audit write failed", exc_info=True)

        # Build response
        return AgentResponse(
            answer=result.output.answer,
            tool_calls=deps.tool_calls,
            render_calls=deps.render_calls,
            unresolved=not deps.tool_calls,
            original_question=question,
            token_usage=TokenUsage(
                input_tokens=usage.input_tokens or 0,
                output_tokens=usage.output_tokens or 0,
            ),
        )
    finally:
        await conn.close()
