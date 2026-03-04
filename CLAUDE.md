# CLAUDE.md

## Project Overview

Deployed financial query agent service (Lambda behind Function URL) that answers natural language questions about spending, income, and transactions. Uses Pydantic AI as the agent framework with predefined parameterized query tools + a constrained SQL fallback. Owns conversation memory (DynamoDB), observability (Logfire), and PII protection (Fernet encryption + regex scrubbing).

**Stack:** Python 3.11+ | Pydantic AI | asyncpg (PostgreSQL) | boto3 (DynamoDB) | Logfire | uv (package manager)

**Spec:** See `docs/finance-query-agent-spec.md` for the full specification (tool signatures, query patterns, security).

## Essential Commands

```bash
# Dependencies
uv sync --all-extras              # Install all deps (including dev)

# Testing
uv run pytest                     # Run all tests
uv run pytest tests/test_tools/   # Run tool tests only
uv run pytest -x                  # Stop on first failure

# Code quality
uv run ruff check .               # Lint
uv run ruff format .              # Format
uv run ruff check . --fix         # Auto-fix lint issues
uv run mypy src/                  # Type check

# Build
uv build                          # Build package
```

## Architecture

```
Browser -> Function URL -> Agent Lambda
                            ├── asyncpg -> RDS (read-only, single connection)
                            ├── Pydantic AI -> LLM API
                            ├── DynamoDB (encrypted conversation history)
                            └── Logfire (PII-scrubbed traces)
```

| What | Where |
|------|-------|
| Lambda handler (Function URL entry point) | `src/finance_query_agent/handler.py` |
| Agent definition (Pydantic AI) | `src/finance_query_agent/agent.py` |
| Settings from env vars | `src/finance_query_agent/config.py` |
| SQL generation from schema mappings | `src/finance_query_agent/query_builder.py` |
| asyncpg single connection (Lambda-aware) | `src/finance_query_agent/connection.py` |
| DynamoDB conversation memory | `src/finance_query_agent/memory.py` |
| Fernet field encryption | `src/finance_query_agent/encryption.py` |
| Regex PII scrubbing | `src/finance_query_agent/redaction.py` |
| Conversation summarization | `src/finance_query_agent/history.py` |
| Logfire + scrubbing callback | `src/finance_query_agent/observability.py` |
| Predefined query tools | `src/finance_query_agent/tools/` |
| Constrained SQL fallback | `src/finance_query_agent/tools/fallback_sql.py` |
| SQL & schema validation | `src/finance_query_agent/validation/` |
| Pydantic models (mapping, results, responses) | `src/finance_query_agent/schemas/` |
| Exception hierarchy | `src/finance_query_agent/exceptions.py` |
| Terraform module | `terraform/` |
| Tests | `tests/` |

## Key Design Decisions

- **Service, not SDK:** Lambda behind Function URL (15 min timeout, no API Gateway 30s limit). Synchronous request-response.
- **Tools-as-wrappers:** The LLM picks a tool and fills params; the service generates and executes parameterized SQL. No raw SQL from the LLM for the common case.
- **Schema mapping:** Declarative `SchemaMapping` config. The service derives all queries from it.
- **Multi-currency:** Results always grouped per currency. Never converts or sums across currencies.
- **User isolation:** Every query scoped to `user_id`. Injected by the service, never by the LLM.
- **Read-only:** No write operations. Enforced at DB role level (security boundary) + keyword rejection (defense-in-depth).
- **PII protection:** Two layers — Fernet encryption at rest (DynamoDB), regex scrubbing in traces (Logfire). No NER models.
- **Single connection:** One `asyncpg.connect()` per invocation (no pool). Matches Lambda's single-request model.

## Code Style

- Run `uv run ruff check . --fix && uv run ruff format .` before commits.
- All queries use parameterized values (`$1`, `$2`). Never string-interpolate user input into SQL.
- This is a service (Lambda). No CLI, no HTTP server framework. Entry point is `handler.handler`.

## Common Gotchas

- `asyncpg` uses `$1` style parameters, not `%s` or `?`.
- `SchemaMapping` validation happens on cold start against the live DB.
- The fallback SQL tool has stricter constraints (no CTEs, no subqueries) than the predefined tools.
- `AmountConvention` determines expense vs income filtering — every spending tool depends on it.
- `AWS_LAMBDA_FUNCTION_NAME` env var is used for Lambda detection (prod vs dev behavior).
- DynamoDB `user_id` must be a separate top-level attribute, not extracted from composite PK.
