"""Lambda handler. Synchronous HTTP request-response."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

from finance_query_agent.exceptions import SchemaValidationError
from finance_query_agent.redaction import sanitize_error

if TYPE_CHECKING:
    from finance_query_agent.schemas.responses import AgentResponse

logger = logging.getLogger(__name__)

_initialized: bool = False


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Lambda handler. Invoked directly via boto3 lambda.invoke()."""
    try:
        result = asyncio.run(_process_request(event))
        return result.model_dump()
    except KeyError as e:
        logger.warning("Missing required field: %s", e)
        return {"error": f"Missing required field: {e}"}
    except ValueError as e:
        logger.warning("Invalid input: %s", e)
        return {"error": f"Invalid input: {e}"}
    except SchemaValidationError as e:
        logger.error("Schema config does not match database: %s", e)
        return {"error": "schema_mismatch", "message": str(e)}
    except Exception as e:
        logger.exception("Agent request failed")
        return {"error": sanitize_error(e)}


async def _process_request(body: dict[str, Any]) -> AgentResponse:
    """Orchestrate agent execution: connect, load history, run, save history."""
    global _initialized  # noqa: PLW0603

    from finance_query_agent.agent import get_agent
    from finance_query_agent.config import get_settings, load_schema_json
    from finance_query_agent.connection import Connection
    from finance_query_agent.encryption import FieldEncryptor
    from finance_query_agent.memory import ConversationMemory
    from finance_query_agent.query_builder import QueryBuilder
    from finance_query_agent.schemas.mapping import SchemaMapping
    from finance_query_agent.schemas.responses import AgentResponse, TokenUsage
    from finance_query_agent.tools import AgentDeps
    from finance_query_agent.validation.schema_validator import validate_schema

    request_start = time.monotonic()

    raw_user_id = body["user_id"]
    session_id = body["session_id"]
    question = body["question"]

    settings = get_settings()

    if not _initialized:
        from finance_query_agent.observability import initialize

        initialize()
        _initialized = True
    encryptor = FieldEncryptor(settings.encryption_key)
    memory = ConversationMemory(settings.dynamodb_table, settings.dynamodb_region, encryptor)
    assert settings.database_url is not None, "database_url must be set"
    conn = Connection(settings.database_url)

    try:
        await conn.connect()

        # Load and validate schema
        schema_data = load_schema_json(settings)
        schema = SchemaMapping(**schema_data)
        type_info = await validate_schema(schema, conn)

        # Cast user_id based on discovered DB column type
        if type_info.user_id_type in ("int2", "int4", "int8", "integer", "bigint", "smallint"):
            if isinstance(raw_user_id, int) and not isinstance(raw_user_id, bool):
                user_id = raw_user_id
            elif isinstance(raw_user_id, str) and raw_user_id.isdigit():
                user_id = int(raw_user_id)
            else:
                raise ValueError(f"user_id must be an integer, got {type(raw_user_id).__name__}: {raw_user_id!r}")
        else:
            user_id = raw_user_id

        # Load conversation history (DynamoDB always uses string keys)
        history = await memory.load_history(str(raw_user_id), session_id)

        # Run agent
        from pydantic_ai import UsageLimits
        from pydantic_ai.exceptions import UsageLimitExceeded
        from pydantic_ai.settings import ModelSettings

        qb = QueryBuilder(schema)
        deps = AgentDeps(connection=conn, query_builder=qb, schema=schema, user_id=user_id)
        agent = get_agent(settings.query_model)

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
            return AgentResponse(
                answer=(
                    "I wasn't able to fully process your question within the time limit."
                    " Please try rephrasing it or breaking it into simpler parts."
                ),
                tool_calls=deps.tool_calls,
                fallback_used=deps.fallback_used,
                fallback_sql=deps.fallback_sql,
                unresolved=True,
                original_question=question,
                token_usage=TokenUsage(input_tokens=0, output_tokens=0),
            )

        # Store updated conversation (DynamoDB always uses string keys)
        await memory.save_history(str(raw_user_id), session_id, result.all_messages())

        # Generate visualizations (best-effort, non-fatal)
        from finance_query_agent.visualization import generate_visualizations

        visualizations = None
        elapsed = time.monotonic() - request_start
        viz_budget = settings.request_budget - elapsed - 1.0  # 1s reserve for response serialization
        if deps.tool_results and viz_budget > 0.5:
            try:
                visualizations = await asyncio.wait_for(
                    generate_visualizations(
                        question=question,
                        tool_results=deps.tool_results,
                        model=settings.viz_model,
                    ),
                    timeout=viz_budget,
                )
            except TimeoutError:
                logger.warning("Visualization timed out (budget=%.1fs)", viz_budget)

        # Build response
        usage = result.usage()
        return AgentResponse(
            answer=result.output,
            tool_calls=deps.tool_calls,
            visualizations=visualizations,
            fallback_used=deps.fallback_used,
            fallback_sql=deps.fallback_sql,
            unresolved=not deps.tool_calls and not deps.fallback_used,
            original_question=question,
            token_usage=TokenUsage(
                input_tokens=usage.input_tokens or 0,
                output_tokens=usage.output_tokens or 0,
            ),
        )
    finally:
        await conn.close()
