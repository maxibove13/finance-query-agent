# CLAUDE.md

## Project Overview

Deployed financial query agent service (Lambda invoked by MPI's backend via boto3) that answers natural language questions about spending, income, and transactions. Uses a **text-to-SQL** architecture: the LLM writes SQL against a documented schema; a governance layer validates every query before execution. A secondary visualization agent generates Vega-Lite chart specs when data is chartable. Owns conversation memory (DynamoDB), audit logging (DynamoDB), observability (Logfire), and PII protection (Fernet encryption + regex scrubbing).

**Primary client:** MPI (My Personal Income) — the frontend/app project at `../my_personal_incomes_ai`.

**Stack:** Python 3.11+ | Pydantic AI | asyncpg (PostgreSQL) | boto3 (DynamoDB) | Logfire | uv (package manager)

**Spec:** See `docs/finance-query-agent-spec.md` for the full specification (tool signatures, query patterns, security).

## AWS

- **AWS Profile:** `my_personal_incomes` (use `--profile my_personal_incomes` for all AWS CLI commands)
- **Region:** `us-west-2`
- **Lambda function name:** `finance-query-agent`

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
Browser -> MPI API Gateway -> MPI Lambda -> boto3 invoke -> Agent Lambda
                                                             ├── asyncpg pool -> RDS (read-only, min 1 / max 5)
                                                             ├── Pydantic AI -> LLM API (primary + secondary model)
                                                             ├── DynamoDB (encrypted conversation history + audit trail)
                                                             └── Logfire (PII-scrubbed traces)
```

| What | Where |
|------|-------|
| Lambda handler (entry point) | `src/finance_query_agent/handler.py` |
| Query agent definition (Pydantic AI) | `src/finance_query_agent/agent.py` |
| Visualization agent (Vega-Lite chart specs) | `src/finance_query_agent/visualization.py` |
| Vega-Lite spec builder (intent → full spec) | `src/finance_query_agent/vega_builder.py` |
| Settings from env vars | `src/finance_query_agent/config.py` |
| Schema context builder (S3 semantic model) | `src/finance_query_agent/schema_builder.py` |
| SQL governance (SELECT-only, LIMIT, EXPLAIN) | `src/finance_query_agent/sql_governance.py` |
| asyncpg connection pool (warm-cached) | `src/finance_query_agent/connection.py` |
| DynamoDB conversation memory | `src/finance_query_agent/memory.py` |
| DynamoDB SQL audit logging | `src/finance_query_agent/audit.py` |
| Fernet field encryption | `src/finance_query_agent/encryption.py` |
| Regex PII scrubbing | `src/finance_query_agent/redaction.py` |
| Conversation summarization | `src/finance_query_agent/history.py` |
| Logfire initialization | `src/finance_query_agent/observability.py` |
| `execute_sql` tool | `src/finance_query_agent/tools/sql.py` |
| Pydantic models (charts, responses) | `src/finance_query_agent/schemas/` |
| Exception hierarchy | `src/finance_query_agent/exceptions.py` |
| Terraform module | `terraform/` |
| Tests | `tests/` |

## Key Design Decisions

- **Service, not SDK:** Lambda invoked by MPI's backend via `boto3 lambda.invoke()` (30s timeout). Synchronous request-response.
- **Text-to-SQL:** Single `execute_sql` tool. The LLM writes SQL against a schema injected into the system prompt; a governance layer validates every query before execution.
- **Schema from S3:** Semantic model (YAML) fetched from S3 at cold start by `schema_builder.py`. No DB introspection — the YAML is the sole source of truth.
- **Two models:** `primary_model` (gpt-4.1) for the query agent, `secondary_model` (gpt-4.1-mini) for visualization and history summarization.
- **Multi-currency:** The semantic model includes `historical_exchange_rates` and `latest_exchange_rates_mv` for SQL-level currency conversion (USD ↔ UYU/ARS/BRL/EUR). The `execute_sql` tool returns raw per-transaction currency values; the LLM formats and groups them in the answer.
- **User isolation:** Every query scoped to `user_id` via PostgreSQL RLS. Injected by the service, never by the LLM.
- **Read-only:** No write operations. Enforced at DB role level (security boundary).
- **PII protection:** Two layers — Fernet encryption at rest (DynamoDB), regex scrubbing in audit logs and traces (Logfire). No NER models.
- **Connection pool:** `asyncpg` pool (min 1, max 5) cached at module level across warm Lambda invocations. Stale pools (loop mismatch) detected and replaced.
- **Audit trail:** Every invocation logged to a DynamoDB audit table with PII-redacted SQL, row counts, timing, and token usage. 90-day TTL.

## Branching

Every feature branch must correspond to a Linear issue. Use the branch name Linear suggests (visible in the issue's `gitBranchName` field):

```
maxibove13/mpi-<NUMBER>-<slug>
```

Include `[MPI-<NUMBER>]` in the PR title and `Closes MPI-<NUMBER>` in the PR body so Linear auto-tracks it.

## Code Style

- Run `uv run ruff check . --fix && uv run ruff format .` before commits.
- This is a service (Lambda). No CLI, no HTTP server framework. Entry point is `handler.handler`.

## Common Gotchas

- `asyncpg` uses `$1` style parameters, not `%s` or `?`.
- Semantic model YAML is fetched from S3 at cold start and cached. Changes require a new deployment or cold start.
- `AWS_LAMBDA_FUNCTION_NAME` env var is used for Lambda detection (prod vs dev behavior).
- DynamoDB `user_id` must be a separate top-level attribute, not extracted from composite PK.
- Governance violations raise `ValueError` → caught as `ModelRetry` so the LLM can self-correct.
