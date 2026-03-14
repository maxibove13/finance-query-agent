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
- Implement a **text-to-SQL** architecture: the LLM writes SQL against a schema injected into the system prompt; a governance layer validates every query before execution.
- **Schema documentation**: the DB schema, business rules, and example queries are defined in a semantic model YAML stored in S3, fetched at cold start by `schema_builder.py` and injected into the system prompt. No DB introspection, no client-side adapter code required.

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
                                 ├── schema_builder.py (S3 semantic model YAML)
                                 ├── asyncpg pool → RDS (read-only)
                                 ├── DynamoDB (encrypted conversation history)
                                 └── Logfire (PII-scrubbed traces)
```

**The service owns:** agent definition, tool definition, SQL governance, prompt engineering, response formatting, database connection management, conversation memory, observability, PII protection.

**The consuming app owns:** authentication, user identity, and Lambda invocation (via boto3).

## 6. Schema Documentation

`schema_builder.py` builds the schema context injected into the system prompt on every invocation. At cold start it fetches the semantic model YAML from S3 (`semantic_model_s3_bucket` / `semantic_model_s3_key`). The YAML is the sole source of truth — no DB introspection. The result is cached for the lifetime of the Lambda instance. The semantic model documents:

- Table names and their purpose
- Column names and types
- Business rules (e.g., how to distinguish expenses from income, what constitutes a transfer)
- Example queries

The LLM uses this context to write valid SQL against the actual schema.

## 7. SQL Tool & Governance

### 7.1 `execute_sql`

The agent has exactly one tool: `execute_sql`. The LLM writes a SELECT query against the schema injected into the system prompt. The tool passes the SQL through the governance layer before executing it.

| Parameter | Type | Description |
|-----------|------|-------------|
| `sql` | `str` | A SELECT query written by the LLM |

Returns: query result rows as a list of dicts.

### 7.2 Governance Pipeline

All LLM-generated SQL passes through the following steps in order before any data is read:

| Step | Description |
|------|-------------|
| `validate_select_only` | Rejects any statement that is not a SELECT (no INSERT, UPDATE, DELETE, DDL, SELECT INTO, writes in CTEs) |
| `cap_limit` | Enforces a maximum of 200 result rows — adds `LIMIT 200` if absent, caps higher values |
| No CROSS JOIN | Blocks CROSS JOINs and implicit comma joins in FROM (accidental cross products) |
| No `set_config()` | Blocks attempts to call PostgreSQL's `set_config()` function (would bypass RLS) |
| `EXPLAIN` pre-flight | Runs `EXPLAIN {sql}` in a readonly transaction to validate the query against the live schema without reading data |
| Readonly transaction | Every query executes inside `conn.transaction(readonly=True)` — a DB-level write guard |
| User scoping via RLS | The service sets `app.user_id` as a session variable; PostgreSQL RLS policies enforce per-user data isolation |
| asyncpg type normalization | Normalizes asyncpg-specific types (Decimal, date/datetime) to JSON-serializable Python types before returning results |

Governance violations raise `ValueError`, which is caught and returned to the LLM as a `ModelRetry` so it can correct the query.

## 8. Agent Configuration

### 8.1 Service Entry Point

The service is deployed as an AWS Lambda invoked by the consuming app's backend via `boto3.client('lambda').invoke()`. The entry point is `handler.handler`. The caller sends a flat JSON payload directly — no `event["body"]` wrapping. The Lambda timeout is 30 seconds to match the API Gateway limit. `user_id` must be a positive integer (int or numeric string).

```
{
  "user_id": 123,
  "session_id": "session-abc",
  "question": "How much did I spend on groceries last month?"
}
```

Configuration is via environment variables (set by Terraform):

| Variable | Description |
|----------|-------------|
| `PRIMARY_MODEL` | Pydantic AI model string for query agent (default: `openai:gpt-4.1`) |
| `SECONDARY_MODEL` | Model for visualization agent + history summarization (default: `openai:gpt-4.1-mini`) |
| `DYNAMODB_TABLE` | DynamoDB table for conversation memory |
| `AUDIT_TABLE` | DynamoDB table for SQL audit trail (optional; `None` = audit disabled) |
| `SEMANTIC_MODEL_S3_BUCKET` | S3 bucket containing the semantic model YAML |
| `SEMANTIC_MODEL_S3_KEY` | S3 key for the semantic model (default: `semantic-model.yaml`) |
| `DB_CREDENTIALS_SECRET_ARN` | Secrets Manager ARN for DB credentials |
| `ENCRYPTION_KEY_SECRET_ARN` | Secrets Manager ARN for Fernet key |
| `LLM_API_KEY_SECRET_ARN` | Secrets Manager ARN for LLM API key |
| `LOGFIRE_TOKEN_SECRET_ARN` | Secrets Manager ARN for Logfire token (optional) |

### 8.1.1 Connection Lifecycle

The Lambda uses an `asyncpg` connection pool (min 1, max 5 connections) cached at module level across warm invocations. `Connection.connect()` creates the pool on first call and reuses it on subsequent warm invocations, terminating stale pools from a previous event loop if detected. The pool uses a 30-second `command_timeout` and `statement_timeout`.

**Database URL format:** Raw `asyncpg` format: `postgresql://user:pass@host:port/dbname`. Resolved from Secrets Manager at runtime (JSON secret with `username`, `password`, `host`, `port`, `dbname`).

### 8.1.2 Exception Hierarchy

```python
class FinanceQueryError(Exception):
    """Base exception for all service errors."""

class DatabaseConnectionError(FinanceQueryError):
    """Database connection error (creation, health, closure)."""

class QueryTimeoutError(FinanceQueryError):
    """A query exceeded the configured timeout."""

class LLMError(FinanceQueryError):
    """LLM API call failed (rate limit, auth, network, unexpected response)."""

class ConversationConflictError(FinanceQueryError):
    """Concurrent write detected: conversation was modified between load and save."""
```

Note: SQL governance violations raise `ValueError`, which is caught and returned to the LLM as a `ModelRetry` so it can self-correct. There is no dedicated `GovernanceError` class.

All exceptions inherit from `FinanceQueryError` so consumers can catch broadly or narrowly.

### 8.2 System Prompt Requirements

The default system prompt MUST:
- Identify the agent as a financial data assistant.
- **Inject the current date** (e.g., "Today is 2026-03-03") so the LLM can resolve relative dates ("last month" -> February 2026).
- **Inject the schema context** (built by `schema_builder.py`) so the LLM knows the database schema.
- Instruct the LLM to resolve relative dates to absolute `date` values before writing SQL.
- Instruct the LLM to always include a LIMIT in every query (governance also enforces a hard cap of 200 rows).
- Instruct the LLM to omit `user_id` conditions (user scoping is enforced by PostgreSQL RLS).
- Instruct the LLM to ask clarifying questions when the user's query is ambiguous.
- Instruct the LLM to format monetary values with currency symbols and two decimal places.
- Instruct the LLM to never fabricate data — if a tool returns empty results, say so.

## 9. Response Format

```python
class AgentResponse(BaseModel):
    answer: str                                        # Natural language answer
    tool_calls: list[ToolCallRecord]                   # Which tools were used, with params
    visualizations: list[VegaLiteChart] | None = None  # Vega-Lite v5 chart specs (null if text-only)
    unresolved: bool                                   # True if the agent couldn't answer
    original_question: str
    token_usage: TokenUsage                            # LLM token consumption

class ToolCallRecord(BaseModel):
    tool_name: str
    parameters: dict
    execution_time_ms: int
    row_count: int

class TokenUsage(BaseModel):
    input_tokens: int
    output_tokens: int

class VegaLiteChart(BaseModel):
    spec: dict[str, Any]  # Full Vega-Lite v5 JSON spec
```

The consuming application decides how to present this to the user (chat UI, API response, etc.).

### 9.1 Agent Output Types

The query agent returns a **union** result type that determines whether visualization runs:

```python
AgentOutput = TextAnswer | AnswerWithVisualization
```

- **`TextAnswer`** — text-only response, no visualization agent runs.
- **`AnswerWithVisualization`** — triggers the visualization agent if the tool results contain ≥ 2 data rows. The visualization agent (running on `secondary_model`) produces `ChartIntent` objects which are converted to full Vega-Lite v5 specs by `vega_builder.py`.

Supported chart types: `bar`, `line`, `pie`, `area`, `scatter`, `heatmap`, `stacked_bar`, `grouped_bar`.

## 10. Multi-Currency Behavior

Financial data often spans multiple currencies. The LLM is aware of this via the schema documentation and is instructed to present multi-currency results clearly (e.g., "You spent $1,200 USD and $45,000 UYU on groceries last month"). The `execute_sql` tool returns raw per-transaction currency values; the LLM formats and groups them in the answer.

## 11. Conversation Memory & History Summarization

Conversation history is stored in DynamoDB (`memory.py`), encrypted at rest with Fernet (`encryption.py`). Each request loads, decrypts, appends, optionally summarizes, re-encrypts, and saves the history.

**History summarization** (`history.py`): When conversation history exceeds 20 messages, older messages (except the most recent 6) are summarized by the `secondary_model` into a 3-5 sentence summary. The summary preserves questions asked, time periods, key totals, and categories discussed while omitting verbose tool output. Tool call/response pairs are kept intact (never split mid-pair). Prior summaries are replaced rather than re-summarized.

## 12. Security Requirements

| Requirement | Description |
|-------------|-------------|
| **S1 — User isolation** | Every query is scoped to a `user_id`. The service sets `app.user_id` as a PostgreSQL session variable; RLS policies enforce per-user data isolation at the DB level. The LLM never controls user scoping. |
| **S2 — No credential exposure** | The `database_url` is resolved from Secrets Manager at runtime. The service never logs or transmits connection strings. |
| **S3 — Read-only** | No tool can modify data. Enforced at DB role level (the security boundary) and reinforced by the SQL governance layer (SELECT-only enforcement). |
| **S4 — Input sanitization** | All queries use parameterized values (`$1`, `$2`, etc.). The governance layer validates SQL structure before execution (AST-level SELECT-only check, LIMIT auto-enforced to max 200 rows, no CROSS JOIN, no `set_config()`), followed by an `EXPLAIN` pre-flight in a readonly transaction. |
| **S5 — PII in LLM context** | Transaction descriptions sent to the LLM may contain PII (merchant names, amounts). The service redacts PII in audit logs (`redaction.py`) and Logfire traces. |

## 13. Observability Requirements

| Requirement | Description |
|-------------|-------------|
| **O1 — Structured logging** | All tool invocations logged with: tool name, parameters, execution time, result row count, success/failure. Uses Python `logging` — no proprietary logging. |
| **O2 — Unresolved query log** | Queries the agent couldn't answer are logged for coverage analysis. |
| **O3 — SQL audit trail** | Every invocation is logged to a DynamoDB audit table (`audit.py`) with PII-redacted SQL queries, row counts, timing, token usage, retry detection, and empty result flags. 90-day TTL. Enabled when `AUDIT_TABLE` is set. |
| **O4 — Cost tracking** | `AgentResponse.token_usage` contains input/output token counts from the LLM call. |
| **O5 — Logfire tracing** | `observability.py` initializes Logfire with a custom scrubbing callback for PII-safe traces. |

## 14. Repository Structure

```
finance-query-agent/
├── src/
│   └── finance_query_agent/
│       ├── __init__.py               <- Package exports
│       ├── handler.py                <- Lambda entry point
│       ├── agent.py                  <- Query agent definition + system prompt
│       ├── visualization.py          <- Visualization agent (Vega-Lite chart generation)
│       ├── vega_builder.py           <- ChartIntent → full Vega-Lite v5 spec
│       ├── config.py                 <- Settings from env vars + Secrets Manager
│       ├── sql_governance.py         <- validate_select_only + cap_limit (governance pipeline)
│       ├── schema_builder.py         <- Schema context builder (S3 semantic model YAML)
│       ├── connection.py             <- asyncpg pool, warm-cached across Lambda invocations
│       ├── memory.py                 <- DynamoDB conversation history
│       ├── audit.py                  <- DynamoDB SQL audit logging
│       ├── encryption.py             <- Fernet field encryption
│       ├── redaction.py              <- Regex PII scrubbing
│       ├── history.py                <- Conversation summarization (LLM-based)
│       ├── observability.py          <- Logfire initialization
│       ├── exceptions.py             <- Exception hierarchy
│       ├── tools/
│       │   └── sql.py                <- execute_sql tool + asyncpg type normalization
│       └── schemas/
│           ├── charts.py             <- ChartIntent + VegaLiteChart models
│           └── responses.py          <- AgentResponse, ToolCallRecord, TokenUsage
├── tests/
├── terraform/
├── pyproject.toml
├── LICENSE                           <- MIT
└── README.md
```

## 15. Integration with my_personal_incomes_ai

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

async def query_agent(user_id: int, session_id: str, question: str) -> dict:
    payload = {"user_id": user_id, "session_id": session_id, "question": question}
    response = lambda_client.invoke(
        FunctionName="finance-query-agent",
        Payload=json.dumps(payload),
    )
    return json.loads(response["Payload"].read())
```

## 16. Design Decisions & Clarifications

### 16.1 Connection Pool Model

The service uses an `asyncpg` connection pool (min 1, max 5) cached at module level across warm Lambda invocations. On cold start the pool is created; on warm invocations it is reused, with stale pools (loop mismatch) detected and replaced. DB credentials are resolved from Secrets Manager on cold start and cached via `lru_cache`.

### 16.2 Governance Error Recovery

When `validate_select_only` rejects a query, the error is returned to the LLM as a tool call failure with a description of the rule that was violated. The LLM can retry with a corrected query. This is intentional — the LLM learns within the conversation what constraints it must respect.

### 16.3 User Isolation via RLS

User scoping is enforced by PostgreSQL Row Level Security. Before executing a query the service sets `app.user_id` as a session variable (`SET LOCAL app.user_id = $1`) inside a readonly transaction. RLS policies on each table filter rows to the current user. The LLM is instructed to omit `user_id` conditions; even if it includes one, RLS enforces the correct boundary independently.

## 17. Open Questions

1. **Currency handling:** The service returns per-currency results from raw queries. Should the LLM present all currencies, or should the system prompt instruct it to highlight the "primary" currency? If so, how is primary currency determined?
