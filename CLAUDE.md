# CLAUDE.md

## Project Overview

Open-source, pip-installable Python SDK (`finance-query-agent`) that lets any application with a financial database answer natural language questions about spending, income, and transactions. Uses Pydantic AI as the agent framework with predefined parameterized query tools + a constrained SQL fallback.

**Stack:** Python 3.11+ | Pydantic AI | asyncpg (PostgreSQL) | uv (package manager)

**Spec:** See `docs/finance-query-agent-spec.md` for the full specification.

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

| What | Where |
|------|-------|
| Public API (`create_agent`, `SchemaMapping`, etc.) | `src/finance_query_agent/__init__.py` |
| Agent definition (Pydantic AI) | `src/finance_query_agent/agent.py` |
| SQL generation from schema mappings | `src/finance_query_agent/query_builder.py` |
| asyncpg connection pool | `src/finance_query_agent/connection.py` |
| Predefined query tools | `src/finance_query_agent/tools/` |
| Constrained SQL fallback | `src/finance_query_agent/tools/fallback_sql.py` |
| SQL & schema validation | `src/finance_query_agent/validation/` |
| Pydantic models (mapping, params, results) | `src/finance_query_agent/schemas/` |
| Tests | `tests/` |

## Key Design Decisions

- **Tools-as-wrappers:** The LLM picks a tool and fills params; the SDK generates and executes parameterized SQL. No raw SQL from the LLM for the common case.
- **Schema mapping:** Clients provide a declarative `SchemaMapping` config. The SDK derives all queries from it. No adapter code.
- **Multi-currency:** Results are always grouped per currency. The SDK never converts or sums across currencies.
- **User isolation:** Every query is scoped to a `user_id`. The SDK injects this — the LLM never controls user scoping.
- **Read-only:** No write operations. Enforced at DB role level (security boundary) + keyword rejection (defense-in-depth).

## Code Style

- Run `uv run ruff check . --fix && uv run ruff format .` before commits.
- All queries use parameterized values (`$1`, `$2`). Never string-interpolate user input into SQL.
- This is a library — no CLI, no HTTP server, no config files. Public API is `create_agent()` + schema models.

## Common Gotchas

- `asyncpg` uses `$1` style parameters, not `%s` or `?`.
- `SchemaMapping` validation happens at `create_agent()` time against the live DB.
- The fallback SQL tool has stricter constraints (no CTEs, no subqueries) than the predefined tools.
- `AmountConvention` determines expense vs income filtering — every spending tool depends on it.
