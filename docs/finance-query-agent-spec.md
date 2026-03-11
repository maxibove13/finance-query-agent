# Finance Query Agent — Specification Requirements

## 1. Context & Motivation

This service originates from [my_personal_incomes_ai](https://github.com/facusorg/my_personal_incomes_ai), a personal finance application that processes bank statements (PDF/CSV), uses AI to extract and categorize transactions, and displays spending analytics. The stack is FastAPI + PostgreSQL on the backend, React + TypeScript on the frontend, with Pydantic AI (OpenAI + Mistral) powering the parsing pipeline.

The app already handles the full import flow — upload, text extraction, AI parsing, keyword-based categorization — and stores structured transaction data across multiple tables (`account_movements`, `credit_card_movements`, `tags`, `accounts`, etc.). What it lacks is a way for users to **ask questions about their data in natural language**.

Rather than building this capability as a tightly coupled feature inside the app, we're extracting it into a standalone service. This serves two purposes:

1. **For the app**: adds a differentiating, monetizable feature — an AI financial assistant that answers spending questions with reliable, auditable results.
2. **For the community**: provides a reusable, deployable service that any application with a financial database can integrate by providing a schema document, without writing query logic.

The service is designed as the first consumer's needs dictate (our app's schema, our data model's quirks), but generic enough that other financial applications can adopt it.

## 2. Problem Statement

Users of financial applications need to ask natural language questions about their data ("How much did I spend on groceries last month?", "Compare my spending this month vs last month"). The service uses a **text-to-SQL** architecture with a governance layer to ensure generated SQL is safe, scoped to the requesting user, and consistent with the documented schema.

## 3. Goals

- Provide a **deployed financial query service** (`finance-query-agent`) that any application with a financial database can integrate via HTTP.
- Use **Pydantic AI** as the agent framework.
- Implement a **text-to-SQL** architecture: the LLM writes SQL against a schema documented in `schema.yaml`; a governance layer validates every query before execution.
- **Schema documentation**: the DB schema, business rules, and example queries are documented in `schema.yaml` and injected into the system prompt. No client-side adapter code required.

## 4. Non-Goals

- No write operations. The agent is strictly read-only.
- No multi-database support in v1. PostgreSQL only.
- No custom tool overrides or extension points.

## 5. Architecture

```
MPI Lambda ──> boto3 invoke ──> Agent Lambda
                                 ├── Pydantic AI Agent
                                 │   └── execute_sql  (single SQL tool)
                                 ├── SQL Governance (validate_select_only)
                                 ├── schema.yaml (DB schema injected into system prompt)
                                 ├── asyncpg → RDS (read-only, single connection)
                                 ├── DynamoDB (encrypted conversation history)
                                 └── Logfire (PII-scrubbed traces)
```

**The service owns:** agent definition, tool definition, SQL governance, prompt engineering, response formatting, database connection management, conversation memory, observability, PII protection.

**The consuming app owns:** authentication, user identity, and Lambda invocation (via boto3).

## 6. Schema Documentation

`schema.yaml` in `src/finance_query_agent/` is the single source of truth for the database schema exposed to the LLM. It documents:

- Table names and their purpose
- Column names and types
- Business rules (e.g., how to distinguish expenses from income, what constitutes a transfer)
- Example queries

It is read at startup and injected verbatim into the system prompt on every invocation. The LLM uses it to write valid SQL against the actual schema.

## 7. SQL Tool & Governance

### 7.1 `execute_sql`

The agent has exactly one tool: `execute_sql`. The LLM writes a SELECT query against the tables documented in `schema.yaml`. The tool passes the SQL through the governance layer before executing it.

| Parameter | Type | Description |
|-----------|------|-------------|
| `sql` | `str` | A SELECT query written by the LLM |

Returns: query result rows as a list of dicts.

### 7.2 `validate_select_only` — Governance Rules

All LLM-generated SQL passes through `validate_select_only` in `sql_governance.py` before touching the database. It enforces six rules:

| Rule | Description |
|------|-------------|
| SELECT-only | Rejects any statement that is not a SELECT (no INSERT, UPDATE, DELETE, DDL) |
| LIMIT required | Every query must include a LIMIT clause |
| user_id injection | The service strips any LLM-supplied `user_id` conditions and injects `WHERE user_id = $1` (parameterized) |
| No CROSS JOIN | Blocks CROSS JOINs and comma-separated table lists in FROM (accidental cross products) |
| No `set_config()` | Blocks attempts to call PostgreSQL's `set_config()` function |
| asyncpg type normalization | Normalizes asyncpg-specific types (e.g., `asyncpg.pgproto.UUID`) to JSON-serializable Python types before returning results |

Any rule violation raises a `GovernanceError` (subclass of `FinanceQueryError`), which is returned to the LLM as a tool error so it can retry with a corrected query.

## 8. Agent Configuration

### 8.1 Service Entry Point

The service is deployed as an AWS Lambda invoked by the consuming app's backend via `boto3.client('lambda').invoke()`. The entry point is `handler.handler`. The caller wraps the payload as `{"body": json.dumps({...})}` to match the handler's event parsing. The Lambda timeout is 30 seconds to match the API Gateway limit.

```
# Payload wrapped in event["body"]

{
  "user_id": "user-123",
  "session_id": "session-abc",
  "question": "How much did I spend on groceries last month?"
}
```

Configuration is via environment variables (set by Terraform):

| Variable | Description |
|----------|-------------|
| `QUERY_MODEL` | Pydantic AI model string (default: `openai:gpt-4o`) |
| `DYNAMODB_TABLE` | DynamoDB table for conversation memory |
| `DB_CREDENTIALS_SECRET_ARN` | Secrets Manager ARN for DB credentials |
| `ENCRYPTION_KEY_SECRET_ARN` | Secrets Manager ARN for Fernet key |
| `LLM_API_KEY_SECRET_ARN` | Secrets Manager ARN for LLM API key |
| `LOGFIRE_TOKEN_SECRET_ARN` | Secrets Manager ARN for Logfire token (optional) |

### 8.1.1 Connection Lifecycle

The Lambda uses a single `asyncpg.connect()` per invocation (no pool). This matches Lambda's single-concurrent-request model. The connection is opened at the start of `_process_request` and closed in a `finally` block.

**Database URL format:** Raw `asyncpg` format: `postgresql://user:pass@host:port/dbname`. Resolved from Secrets Manager at runtime (JSON secret with `username`, `password`, `host`, `port`, `dbname`).

### 8.1.2 Exception Hierarchy

```python
class FinanceQueryError(Exception):
    """Base exception for all service errors."""

class GovernanceError(FinanceQueryError):
    """LLM-generated SQL failed governance validation."""

class DatabaseConnectionError(FinanceQueryError):
    """Database connection error (creation, health, closure)."""

class QueryTimeoutError(FinanceQueryError):
    """A query exceeded the configured timeout."""

class LLMError(FinanceQueryError):
    """LLM API call failed (rate limit, auth, network, unexpected response)."""
```

All exceptions inherit from `FinanceQueryError` so consumers can catch broadly or narrowly.

### 8.2 Hooks

**`pre_llm_hook`** — Called before tool results are sent back to the LLM. Use for PII redaction.

```python
class PreLlmHookContext:
    tool_name: str
    tool_results: list[dict]     # The rows about to be sent to the LLM

# Return a modified PreLlmHookContext. The service sends the returned version to the LLM.
# Must be synchronous. Must not raise — if it does, the tool call fails.
```

**`on_tool_call`** — Called after each tool execution completes. Use for tracing (Langfuse, OpenTelemetry, etc.).

```python
class ToolCallEvent:
    tool_name: str
    parameters: dict
    execution_time_ms: int
    row_count: int
    success: bool
    error: str | None

# Fire-and-forget. Must be synchronous. Exceptions are logged and swallowed.
```

### 8.3 System Prompt Requirements

The default system prompt MUST:
- Identify the agent as a financial data assistant.
- **Inject the current date** (e.g., "Today is 2026-03-03") so the LLM can resolve relative dates ("last month" -> February 2026).
- **Inject the full contents of `schema.yaml`** so the LLM knows the database schema.
- Instruct the LLM to resolve relative dates to absolute `date` values before writing SQL.
- Instruct the LLM to always include a LIMIT in every query.
- Instruct the LLM to never include `user_id` conditions (the service injects them).
- Instruct the LLM to ask clarifying questions when the user's query is ambiguous.
- Instruct the LLM to format monetary values with currency symbols and two decimal places.
- Instruct the LLM to never fabricate data — if a tool returns empty results, say so.
- Be fully replaceable via `system_prompt_override`.

## 9. Response Format

```python
class AgentResponse(BaseModel):
    answer: str                          # Natural language answer
    tool_calls: list[ToolCallRecord]     # Which tools were used, with params
    unresolved: bool                     # True if the agent couldn't answer
    original_question: str
    token_usage: TokenUsage              # LLM token consumption

class ToolCallRecord(BaseModel):
    tool_name: str
    parameters: dict
    execution_time_ms: int
    row_count: int

class TokenUsage(BaseModel):
    input_tokens: int
    output_tokens: int
```

The consuming application decides how to present this to the user (chat UI, API response, etc.).

## 10. Multi-Currency Behavior

Financial data often spans multiple currencies. The LLM is aware of this via the schema documentation and is instructed to present multi-currency results clearly (e.g., "You spent $1,200 USD and $45,000 UYU on groceries last month"). The `execute_sql` tool returns raw per-transaction currency values; the LLM formats and groups them in the answer.

## 11. Security Requirements

| Requirement | Description |
|-------------|-------------|
| **S1 — User isolation** | Every query is scoped to a `user_id`. The governance layer strips any LLM-supplied `user_id` conditions and injects `WHERE user_id = $1` (parameterized). The LLM never controls user scoping. |
| **S2 — No credential exposure** | The `database_url` is resolved from Secrets Manager at runtime. The service never logs or transmits connection strings. |
| **S3 — Read-only** | No tool can modify data. Enforced at DB role level (the security boundary) and reinforced by the SQL governance layer (SELECT-only enforcement). |
| **S4 — Input sanitization** | All queries use parameterized values (`$1`, `$2`, etc.). The governance layer validates SQL structure before execution (AST-level SELECT-only check, LIMIT required, no CROSS JOIN, no `set_config()`). |
| **S5 — PII in LLM context** | Transaction descriptions sent to the LLM may contain PII (merchant names, amounts). The consuming application is responsible for PII handling policy via the `pre_llm_hook`. |

## 12. Observability Requirements

| Requirement | Description |
|-------------|-------------|
| **O1 — Structured logging** | All tool invocations logged with: tool name, parameters, execution time, result row count, success/failure. Uses Python `logging` — no proprietary logging. |
| **O2 — Unresolved query log** | Queries the agent couldn't answer are logged for coverage analysis. |
| **O3 — Tracing hooks** | `on_tool_call` callback fires after each tool execution with a `ToolCallEvent`. Synchronous, fire-and-forget, exceptions swallowed. The consuming application bridges this to Langfuse/OpenTelemetry/etc. |
| **O4 — Cost tracking** | `AgentResponse.token_usage` contains input/output token counts from the LLM call. |

## 13. Repository Structure

```
finance-query-agent/
├── src/
│   └── finance_query_agent/
│       ├── __init__.py               <- Package exports and exceptions
│       ├── agent.py                  <- Pydantic AI agent definition + system prompt
│       ├── sql_governance.py         <- validate_select_only (6 governance rules)
│       ├── schema.yaml               <- DB schema documentation injected into system prompt
│       ├── connection.py             <- asyncpg single connection (Lambda-aware)
│       ├── tools/
│       │   └── sql.py                <- execute_sql tool + asyncpg type normalization
│       └── schemas/
│           ├── charts.py             <- Chart specs (pie, bar, line, grouped_bar)
│           └── responses.py          <- AgentResponse, ToolCallRecord, TokenUsage
├── tests/
│   ├── test_governance.py            <- Unit tests for SQL governance rules
│   ├── test_tools/
│   ├── test_agent.py
│   └── conftest.py
├── pyproject.toml
├── LICENSE                           <- MIT
└── README.md
```

## 14. Integration with my_personal_incomes_ai

The consuming app deploys the finance-query-agent as an AWS Lambda via the Terraform module. MPI's backend invokes the agent Lambda directly via `boto3.client('lambda').invoke()`.

**Terraform integration:**

```hcl
module "finance_agent" {
  source = "../finance-query-agent/terraform"

  ecr_image_uri = "${module.finance_agent.ecr_repository_url}:latest"
}
```

**Backend integration** (MPI Lambda invokes agent Lambda via boto3):

```python
# app/services/finance_agent_service.py
import json
import boto3

lambda_client = boto3.client("lambda")

async def query_agent(user_id: str, session_id: str, question: str) -> dict:
    payload = {"user_id": user_id, "session_id": session_id, "question": question}
    response = lambda_client.invoke(
        FunctionName="finance-query-agent",
        Payload=json.dumps({"body": json.dumps(payload)}),
    )
    result = json.loads(response["Payload"].read())
    return json.loads(result["body"])
```

## 15. Design Decisions & Clarifications

### 15.1 Single Connection Model

The service uses a single `asyncpg.connect()` per Lambda invocation instead of a connection pool. This matches Lambda's execution model (one concurrent request per instance). The connection is created at request start and closed in a `finally` block. DB credentials are resolved from Secrets Manager on cold start and cached via `lru_cache`.

### 15.2 Governance Error Recovery

When `validate_select_only` rejects a query, the error is returned to the LLM as a tool call failure with a description of the rule that was violated. The LLM can retry with a corrected query. This is intentional — the LLM learns within the conversation what constraints it must respect.

### 15.3 user_id Injection

The governance layer strips any `WHERE user_id = ...` condition written by the LLM and replaces it with a parameterized `WHERE user_id = $1` using the authenticated `user_id` from the Lambda event. This prevents the LLM from querying another user's data even if prompted to do so.

## 16. Open Questions

1. **Currency handling:** The service returns per-currency results from raw queries. Should the LLM present all currencies, or should the system prompt instruct it to highlight the "primary" currency? If so, how is primary currency determined?
