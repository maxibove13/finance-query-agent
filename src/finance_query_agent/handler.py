"""Lambda Function URL handler. Synchronous HTTP request-response."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Any

from finance_query_agent.redaction import sanitize_error

if TYPE_CHECKING:
    from finance_query_agent.schemas.responses import AgentResponse

logger = logging.getLogger(__name__)

_initialized: bool = False


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Lambda Function URL handler. Receives HTTP request, returns HTTP response."""
    try:
        body = json.loads(event.get("body", "{}"))
        result = asyncio.run(_process_request(body))
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": result.model_dump_json(),
        }
    except KeyError as e:
        logger.warning("Missing required field: %s", e)
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": f"Missing required field: {e}"}),
        }
    except Exception as e:
        logger.exception("Agent request failed")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": sanitize_error(e)}),
        }


async def _process_request(body: dict[str, Any]) -> AgentResponse:
    """Orchestrate agent execution: connect, load history, run, save history."""
    global _initialized  # noqa: PLW0603
    if not _initialized:
        from finance_query_agent.observability import initialize

        initialize()
        _initialized = True

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

    # user_id comes from the authenticated caller (AWS IAM on Function URL)
    user_id = body["user_id"]
    session_id = body["session_id"]
    question = body["question"]

    settings = get_settings()
    encryptor = FieldEncryptor(settings.encryption_key)
    memory = ConversationMemory(settings.dynamodb_table, settings.dynamodb_region, encryptor)
    assert settings.database_url is not None, "database_url must be set"
    conn = Connection(settings.database_url)

    try:
        await conn.connect()

        # Load and validate schema
        schema_data = load_schema_json(settings)
        schema = SchemaMapping(**schema_data)
        await validate_schema(schema, conn)

        # Load conversation history
        history = await memory.load_history(user_id, session_id)

        # Run agent
        qb = QueryBuilder(schema)
        deps = AgentDeps(connection=conn, query_builder=qb, schema=schema, user_id=user_id)
        agent = get_agent(settings.llm_model)
        result = await agent.run(question, deps=deps, message_history=history)

        # Store updated conversation
        await memory.save_history(user_id, session_id, result.all_messages())

        # Build response
        usage = result.usage()
        return AgentResponse(
            answer=result.output,
            tool_calls=deps.tool_calls,
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
