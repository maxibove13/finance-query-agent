# Finance Query Agent — Specification Requirements

## 1. Context & Motivation

This service originates from [my_personal_incomes_ai](https://github.com/facusorg/my_personal_incomes_ai), a personal finance application that processes bank statements (PDF/CSV), uses AI to extract and categorize transactions, and displays spending analytics. The stack is FastAPI + PostgreSQL on the backend, React + TypeScript on the frontend, with Pydantic AI (OpenAI + Mistral) powering the parsing pipeline.

The app already handles the full import flow — upload, text extraction, AI parsing, keyword-based categorization — and stores structured transaction data across multiple tables (`account_movements`, `credit_card_movements`, `tags`, `accounts`, etc.). What it lacks is a way for users to **ask questions about their data in natural language**.

Rather than building this capability as a tightly coupled feature inside the app, we're extracting it into a standalone service. This serves two purposes:

1. **For the app**: adds a differentiating, monetizable feature — an AI financial assistant that answers spending questions with reliable, auditable results.
2. **For the community**: provides a reusable, deployable service that any application with a financial database can integrate by providing a schema mapping, without writing query logic.

The service is designed as the first consumer's needs dictate (our app's schema, our data model's quirks), but generic enough that other financial applications can adopt it.

## 2. Problem Statement

Users of financial applications need to ask natural language questions about their data ("How much did I spend on groceries last month?", "Compare my spending this month vs last month"). Building this as raw text-to-SQL is unreliable — wrong JOINs, hallucinated column names, and plausible-but-incorrect results erode trust. A tools-based agent with predefined, parameterized query operations provides reliability for the common case, while a constrained SQL fallback covers the long tail.

## 3. Goals

- Provide a **deployed financial query service** (`finance-query-agent`) that any application with a financial database can integrate via HTTP.
- Use **Pydantic AI** as the agent framework.
- Implement a **tools-as-wrappers** architecture: the LLM selects which tool to call and with what parameters; the tool executes a predefined, parameterized query.
- Include a **constrained SQL generation tool** as a fallback for queries not covered by predefined tools.
- **Configuration-driven integration**: clients provide a declarative schema mapping (table names, column names, joins). The service generates all queries internally. No adapter code to write.

## 4. Non-Goals

- Not a BI/analytics platform. No dashboards, no visualizations, no semantic layer.
- No write operations. The agent is strictly read-only.
- No multi-database support in v1. PostgreSQL only. The schema mapping approach allows future database backends.
- No custom tool overrides or extension points. The service provides a fixed set of tools. If a question can't be answered by those tools, the constrained SQL fallback handles it.

## 5. Architecture

```
MPI Lambda ──> boto3 invoke ──> Agent Lambda
                                 ├── Pydantic AI Agent
                                 │   ├── query_expenses          (view-backed)
                                 │   ├── query_income            (view-backed)
                                 │   ├── query_balance_history   (view-backed)
                                 │   ├── search_transactions
                                 │   ├── get_recurring_expenses
                                 │   └── [fallback] run_constrained_query
                                 ├── Materialized Views (pre-computed, with currency conversion)
                                 ├── Query Builder (SchemaMapping → parameterized SQL)
                                 ├── asyncpg → RDS (read-only, single connection)
                                 ├── DynamoDB (encrypted conversation history)
                                 └── Logfire (PII-scrubbed traces)
```

**The service owns:** agent definition, tool definitions, query building, prompt engineering, response formatting, SQL validation, database connection management, conversation memory, observability, PII protection.

**The consuming app owns:** schema mapping configuration (via Terraform), authentication, user identity, and Lambda invocation (via boto3).

## 6. Schema Mapping (Client Integration)

This is the only thing a client needs to provide. A declarative configuration that tells the service where financial data lives in their database. Stored in SSM Parameter Store at `/<project-name>/schema-config` (or locally via `SCHEMA_CONFIG_JSON` env var / `SCHEMA_CONFIG_PATH` file).

### 6.1 Configuration Model

```python
from finance_query_agent import (
    SchemaMapping,
    TableMapping,
    JoinDef,
    ColumnRef,
    AmountConvention,
)

schema = SchemaMapping(
    # Primary transaction table (required)
    transactions=TableMapping(
        table="account_movements",
        columns={
            "date": "issued_at",
            "amount": "amount",
            "description": "description",
            "user_id": ColumnRef("accounts", "user_id"),   # lives on parent table
            "currency": ColumnRef("accounts", "currency"),  # lives on parent table
            "account_id": "account_id",
            "balance": "balance",                           # running balance after tx
        },
        joins=[
            JoinDef(
                table="accounts",
                on="account_movements.account_id = accounts.id",
                type="inner",
            ),
            JoinDef(
                table="tags",
                on="account_movements.category_id = tags.id",
                type="left",  # nullable category_id — LEFT JOIN preserves uncategorized
            ),
        ],
        amount_convention=AmountConvention(
            # How to distinguish expenses from income
            direction_column="movement_direction",
            expense_value="debit",
            income_value="credit",
        ),
    ),

    # Category table (required)
    categories=TableMapping(
        table="tags",
        columns={
            "id": "id",
            "name": "name",
        },
        # No user_id — categories are shared/global. Marked explicitly:
        user_scoped=False,
    ),

    # Account table (required)
    accounts=TableMapping(
        table="accounts",
        columns={
            "id": "id",
            "name": "alias",                # nullable — some accounts may not have a display name
            "user_id": "user_id",
        },
    ),

    # Secondary transaction table (optional — e.g. credit cards)
    secondary_transactions=TableMapping(
        table="credit_card_movements",
        columns={
            "date": "issued_at",
            "amount": "amount",
            "description": "description",
            "user_id": ColumnRef("credit_cards", "user_id"),
            "currency": "currency",            # direct column here
            "account_id": "credit_card_id",
        },
        joins=[
            JoinDef(
                table="credit_cards",
                on="credit_card_movements.credit_card_id = credit_cards.id",
                type="inner",
            ),
            JoinDef(
                table="tags",
                on="credit_card_movements.category_id = tags.id",
                type="left",
            ),
        ],
        amount_convention=AmountConvention(
            direction_column="movement_direction",
            expense_value="debit",
            income_value="credit",
        ),
    ),
)
```

### 6.2 `ColumnRef` — Referencing Columns on Joined Tables

When a column lives on a parent table (not directly on the transaction table), use `ColumnRef`:

```python
class ColumnRef:
    table: str   # The joined table name
    column: str  # The column on that table
```

The service resolves `ColumnRef` by finding the matching `JoinDef` in the table's `joins` list. If no join to the referenced table exists, schema validation fails at startup.

### 6.3 `AmountConvention` — Expense vs. Income

Financial databases represent transaction direction differently. The service supports two conventions:

```python
class AmountConvention:
    # Option A: separate direction column (e.g. CREDIT/DEBIT enum)
    direction_column: str | None = None
    expense_value: str | None = None
    income_value: str | None = None

    # Option B: sign-based (positive = expense or positive = income)
    sign_means_expense: Literal["positive", "negative"] | None = None
```

Exactly one of the two options must be set — schema validation rejects configurations where both are set or neither is set. The service uses this to generate the correct `WHERE` clause when filtering for expenses (all spending tools) or income. When a tool needs "total spending," the service filters to expenses only. When a tool needs "all transactions" (e.g., `search_transactions`), no direction filter is applied.

### 6.4 `JoinDef` — Table Joins

```python
class JoinDef:
    table: str                                    # Table to join
    on: str                                       # Join condition
    type: Literal["inner", "left"] = "left"       # Join type (default LEFT to preserve rows)
```

Each `TableMapping` has a `joins` list. The query builder applies all joins when querying that table. When building `UNION ALL` across primary and secondary transactions, each side uses its own join definitions independently.

### 6.5 Required Column Mappings

| Concept | Column key | Type | Where | Description |
|---------|-----------|------|-------|-------------|
| Transaction date | `date` | `date` or `timestamp` | transactions | When the transaction occurred |
| Amount | `amount` | `numeric` | transactions | Transaction amount |
| Description | `description` | `text` | transactions | Merchant name or transaction description |
| Currency | `currency` | `text` | transactions or via `ColumnRef` | ISO currency code |
| User ID | `user_id` | `text` or `uuid` | transactions or via `ColumnRef` | Row-level user ownership |
| Account ID | `account_id` | `text` or `uuid` | transactions | FK to accounts table |
| Category ID | `id` (on categories) | `text` or `uuid` | categories | Category primary key |
| Category name | `name` (on categories) | `text` | categories | Human-readable category name |

Optional columns:

| Concept | Column key | Type | Description |
|---------|-----------|------|-------------|
| Balance | `balance` | `numeric` | Running balance after transaction |

### 6.6 `ViewMapping` — Pre-Computed Materialized Views

The `SchemaMapping` supports three optional `ViewMapping` fields for pre-computed materialized views. When configured, they enable the view-backed tools (`query_expenses`, `query_income`, `query_balance_history`).

```python
class ViewMapping(BaseModel):
    """Mapping for a pre-computed database view (e.g. materialized view with pre-joined exchange rates)."""
    table: str
    columns: dict[str, str]  # logical key -> actual column name
```

Each `ViewMapping` requires specific logical keys:

| Field | Required logical keys |
|-------|----------------------|
| `unified_expenses` | `user_id`, `date`, `usd_amount`, `local_amount`, `category`, `merchant` |
| `unified_income` | `user_id`, `month`, `usd_amount`, `local_amount` |
| `unified_balances` | `user_id`, `date`, `usd_total`, `local_total` |

These views are expected to contain pre-converted currency amounts (USD and local), pre-filtered data (e.g., excluding internal transfers), and pre-joined categories/merchants. The service validates required logical keys at startup.

### 6.7 `user_scoped` Flag

Tables that are shared/global (no `user_id` column) MUST set `user_scoped=False`. Default is `True`. The service will NOT inject user filtering on tables marked `user_scoped=False`. User isolation on transaction queries comes from the `user_id` mapping on the transactions table (whether direct or via `ColumnRef`).

### 6.8 What the Service Derives from the Mapping

| Service gets | From |
|----------|------|
| View-backed tool queries | `ViewMapping` (unified_expenses, unified_income, unified_balances) |
| Direct query tool SQL | Column mappings + join definitions + amount convention |
| Expense/income filtering | `AmountConvention` on each transaction table |
| Fallback SQL table/column allowlist | All mapped tables and columns (nothing else is queryable) |
| Schema description for LLM context | Column names, types (introspected from DB), relationships |
| User isolation `WHERE` clauses | The `user_id` column mapping (direct or via `ColumnRef` + JOIN) |
| `UNION ALL` for multi-source queries | `transactions` + `secondary_transactions` with independent JOINs |

### 6.9 Schema Validation

On startup (first request), the service MUST:
1. Connect to the database and verify all mapped tables and columns exist (including `ViewMapping` tables).
2. For tables with `user_scoped=True` (default): verify the `user_id` column exists, either directly or as a `ColumnRef` with a valid join path.
3. For tables with `user_scoped=False`: skip user_id validation.
4. Verify all `JoinDef` conditions reference valid columns on both sides.
5. Verify all `ColumnRef` entries point to a table that has a corresponding `JoinDef`.
6. Verify `AmountConvention` is set on every transaction table, references valid columns, and has exactly one of the two convention options set (direction column OR sign-based, not both, not neither).
7. Verify each `ViewMapping` has all required logical keys for its field.
8. Raise a clear error if any mapping is invalid, specifying exactly which table/column is wrong.

## 7. Predefined Tools

The agent has 6 tools: 3 view-backed aggregation tools, 2 direct query tools, and 1 constrained SQL fallback. The agent selects the tool and fills the parameters; the service generates and executes the SQL.

View-backed tools (`query_expenses`, `query_income`, `query_balance_history`) query pre-computed materialized views configured via `ViewMapping`. They are conditionally registered — if the corresponding `ViewMapping` is not set in the `SchemaMapping`, the tool is hidden from the agent via a prepare callback that returns `None`.

Direct query tools (`search_transactions`, `get_recurring_expenses`) use the `QueryBuilder` to generate parameterized SQL from the `SchemaMapping`.

### 7.1 `query_expenses`

Aggregates expenses over a date range from a pre-computed materialized view. Replaces the previous `get_spending_by_category`, `get_monthly_totals`, `get_top_merchants`, `compare_periods`, `get_spending_trend`, and `get_category_breakdown` tools via the `group_by` parameter. Internal transfers and credit card payment double-counting are excluded by the view.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `period_start` | `date` | yes | Start of period (inclusive) |
| `period_end` | `date` | yes | End of period (inclusive) |
| `group_by` | `"category" \| "month" \| "merchant" \| "total"` | no | Aggregation dimension (default `"total"`) |
| `currency` | `"usd" \| "local"` | no | Pre-converted currency amounts (default `"usd"`) |
| `category` | `str \| None` | no | Exact match filter on category |
| `merchant` | `str \| None` | no | Substring match on merchant (ILIKE) |
| `limit` | `int \| None` | no | Max rows returned |

Returns: `list[ExpenseGroup]` — each with `label`, `total_amount`, `transaction_count`, `currency`.

**Conditional registration:** Only registered if `schema.unified_expenses` (`ViewMapping`) is configured.

### 7.2 `query_income`

Monthly income totals over a date range from a pre-computed materialized view.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `period_start` | `date` | yes | Start of period (inclusive) |
| `period_end` | `date` | yes | End of period (inclusive) |
| `currency` | `"usd" \| "local"` | no | Pre-converted currency amounts (default `"usd"`) |

Returns: `list[IncomeMonth]` — each with `month_label` (`"YYYY/MM"`), `total_amount`, `currency`.

**Conditional registration:** Only registered if `schema.unified_income` (`ViewMapping`) is configured.

### 7.3 `query_balance_history`

Balance snapshots from pre-computed materialized view, optionally with per-currency breakdown.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `period_start` | `date \| None` | no | Start of range. Omit for latest snapshot only. |
| `period_end` | `date \| None` | no | End of range. Omit for latest snapshot only. |
| `currency` | `"usd" \| "local"` | no | Total balance currency (default `"usd"`) |
| `include_breakdown` | `bool` | no | Include per-currency JSONB breakdown (default `false`) |
| `granularity` | `"daily" \| "monthly"` | no | `"monthly"` returns last snapshot per month (default `"monthly"`) |

Returns: `list[BalanceSnapshot]` — each with `date`, `total_balance`, `currency_balances` (optional `dict[str, Decimal]`).

**Conditional registration:** Only registered if `schema.unified_balances` (`ViewMapping`) is configured.

### 7.4 `search_transactions`

Searches individual transactions by description, amount range, date range, or category. Returns all transactions (expenses and income) unless filtered. Uses `QueryBuilder` to generate parameterized SQL from `SchemaMapping`.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | `str \| None` | no | Text search on description (case-insensitive `ILIKE '%query%'`) |
| `period_start` | `date \| None` | no | Start of period |
| `period_end` | `date \| None` | no | End of period |
| `min_amount` | `float \| None` | no | Minimum absolute amount |
| `max_amount` | `float \| None` | no | Maximum absolute amount |
| `category` | `str \| None` | no | Filter by category name |
| `direction` | `"expense" \| "income" \| None` | no | Filter by direction. `None` = both. |
| `limit` | `int` | no | Max results (default 20) |
| `offset` | `int` | no | Pagination offset |

Returns: `TransactionSearchResult` — with `transactions: list[Transaction]`, `total_count`, `has_more`. Each `Transaction` includes `date`, `amount`, `description`, `currency`, `category`.

### 7.5 `get_recurring_expenses`

Identifies recurring transactions (subscriptions, regular payments). Only counts expenses. Uses `QueryBuilder` to generate parameterized SQL from `SchemaMapping`.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `period_start` | `date` | yes | Start of analysis window |
| `period_end` | `date` | yes | End of analysis window |
| `min_occurrences` | `int` | no | Minimum times a charge must appear (default 3) |

**Detection algorithm:**

1. Group expense transactions by normalized description (lowercase, trimmed).
2. For each group with >= `min_occurrences` transactions in the period:
   a. Compute the median amount and the median interval (in days) between consecutive transactions.
   b. Classify frequency based on median interval:
      - 5-10 days: `"weekly"`
      - 25-35 days: `"monthly"`
      - 340-395 days: `"yearly"`
      - Outside these ranges: `"irregular"` (excluded from results)
   c. Exclude groups where the coefficient of variation of intervals > 0.5 (too inconsistent to be a subscription).
3. Return results sorted by `total_amount` descending.

Returns: `list[RecurringExpense]` — each with `merchant_name`, `estimated_amount` (median), `frequency`, `occurrences`, `total_amount`, `currency`.

## 8. Constrained SQL Generation Tool (Fallback)

### 8.1 Purpose

Handles queries that don't map to any predefined tool. The agent generates SQL, but under strict constraints that make it safe for production use.

### 8.2 Requirements

**R1 — Read-only enforcement.** Two layers:
  - Layer 1 (defense-in-depth): Regex-based pre-filter rejects SQL containing DML, DDL, or dangerous keywords: `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `TRUNCATE`, `CREATE`, `GRANT`, `REVOKE`, `COPY`, `DO`, `EXECUTE`, `CALL`, `SET`, `RESET`, `LISTEN`, `NOTIFY`, `LOAD`, `VACUUM`, `REINDEX`. This is not a security boundary — it catches obvious mistakes and reduces attack surface.
  - Layer 2 (security boundary): The database connection MUST use a read-only PostgreSQL role. This is the actual enforcement.

**R2 — Table and column whitelisting.** The allowlist is derived automatically from the `SchemaMapping` — only mapped tables and columns are queryable. The tool MUST reject queries referencing anything outside the mapping.

**R3 — EXPLAIN validation.** Before executing any generated query, run `EXPLAIN` on it. If `EXPLAIN` fails (syntax error, invalid reference), return the error to the LLM for self-correction via Pydantic AI's `ModelRetry`. Maximum 3 retry attempts.

**R4 — Query timeout.** All queries executed by this tool MUST have a hard timeout of 30 seconds (configurable). Use `statement_timeout` at the session level.

**R5 — Result set limit.** The tool MUST inject `LIMIT 200` (configurable) if the generated SQL does not already contain a `LIMIT` clause.

**R6 — Schema context injection.** The tool's prompt includes the whitelisted table schemas (column names, types, descriptions, foreign key relationships) derived from the `SchemaMapping` and introspected from the database.

**R7 — No subqueries in v1.** LLM-generated SQL must be a single `SELECT` statement. CTEs, subqueries, `DO` blocks, and multiple statements are rejected. This constraint can be relaxed in future versions after validation. (Note: this restriction applies only to the fallback tool, not to the service's predefined tools which use CTEs/subqueries internally.)

**R8 — User isolation injection.** The tool MUST automatically inject user scoping using the `user_id` mapping (resolving `ColumnRef` + JOINs as needed). The LLM-generated SQL MUST NOT contain any `user_id` condition — the service strips any LLM-generated user filtering and replaces it with its own.

**R9 — Audit logging.** Every query generated by this tool MUST be logged with: the original natural language question, the generated SQL, whether it passed validation, whether it was executed, the execution time, and the row count returned.

**R10 — Unresolved query tracking.** If the fallback tool also fails (after retries), the original question MUST be logged as an "unresolved query" for later analysis. This is the feedback loop for identifying new predefined tools to build.

## 9. Agent Configuration

### 9.1 Service Entry Point

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
| `SCHEMA_CONFIG_SSM_PARAM` | SSM parameter name for SchemaMapping JSON (set by Terraform) |
| `QUERY_MODEL` | Pydantic AI model string (default: `openai:gpt-4o`) |
| `DYNAMODB_TABLE` | DynamoDB table for conversation memory |
| `DB_CREDENTIALS_SECRET_ARN` | Secrets Manager ARN for DB credentials |
| `ENCRYPTION_KEY_SECRET_ARN` | Secrets Manager ARN for Fernet key |
| `LLM_API_KEY_SECRET_ARN` | Secrets Manager ARN for LLM API key |
| `LOGFIRE_TOKEN_SECRET_ARN` | Secrets Manager ARN for Logfire token (optional) |

### 9.1.1 Connection Lifecycle

The Lambda uses a single `asyncpg.connect()` per invocation (no pool). This matches Lambda's single-concurrent-request model. The connection is opened at the start of `_process_request` and closed in a `finally` block.

**Database URL format:** Raw `asyncpg` format: `postgresql://user:pass@host:port/dbname`. Resolved from Secrets Manager at runtime (JSON secret with `username`, `password`, `host`, `port`, `dbname`).

### 9.1.2 `run()` Method

```python
async def run(
    self,
    question: str,              # Natural language question
    user_id: str,               # User ID for row-level isolation
) -> AgentResponse:
    """
    Run the agent on a natural language question.

    Raises:
        FinanceQueryError: Base exception for all service errors.
        DatabaseConnectionError: Pool not connected or DB unreachable.
        QueryTimeoutError: A query exceeded the configured timeout.
        LLMError: LLM API call failed (rate limit, auth, network).
        SchemaValidationError: Schema mapping is invalid (raised during connect()).
    """
```

### 9.1.3 Exception Hierarchy

```python
class FinanceQueryError(Exception):
    """Base exception for all service errors."""

class SchemaValidationError(FinanceQueryError):
    """Schema mapping does not match the live database."""

class DatabaseConnectionError(FinanceQueryError):
    """Database connection pool error (creation, health, closure)."""

class QueryTimeoutError(FinanceQueryError):
    """A query exceeded the configured timeout."""

class LLMError(FinanceQueryError):
    """LLM API call failed (rate limit, auth, network, unexpected response)."""
```

All exceptions inherit from `FinanceQueryError` so consumers can catch broadly or narrowly. Raw `asyncpg` and `httpx` exceptions are never surfaced directly.

### 9.2 Hooks

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

### 9.3 System Prompt Requirements

The default system prompt MUST:
- Identify the agent as a financial data assistant.
- **Inject the current date** (e.g., "Today is 2026-03-03") so the LLM can resolve relative dates ("last month" -> February 2026).
- Instruct the LLM to resolve relative dates to absolute `date` values before calling tools.
- Instruct the LLM to prefer predefined tools over the SQL fallback.
- Instruct the LLM to ask clarifying questions (via a structured response) when the user's query is ambiguous, rather than guessing.
- Instruct the LLM to format monetary values with currency symbols and two decimal places.
- Instruct the LLM to never fabricate data — if a tool returns empty results, say so.
- Be fully replaceable via `system_prompt_override`.

## 10. Response Format

```python
class AgentResponse(BaseModel):
    answer: str                          # Natural language answer
    tool_calls: list[ToolCallRecord]     # Which tools were used, with params
    fallback_used: bool                  # Whether SQL fallback was invoked
    fallback_sql: str | None             # The generated SQL if fallback was used
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

## 11. Multi-Currency Behavior

Financial data often spans multiple currencies. The service handles this consistently across all tools:

- **View-backed tools** (`query_expenses`, `query_income`, `query_balance_history`): Support a `currency` parameter (`"usd"` or `"local"`) selecting pre-converted amounts from the materialized view. Results are returned in the selected currency denomination.
- **Search tools** (`search_transactions`): Return the raw currency per transaction.
- **Balance tool** (`query_balance_history`): Can optionally include a per-currency breakdown via `include_breakdown=True`.
- **Recurring tool** (`get_recurring_expenses`): Groups by (description, currency) — a Netflix charge in USD and one in EUR are separate recurring items.
- **LLM formatting**: The system prompt instructs the LLM to present multi-currency results clearly (e.g., "You spent $1,200 USD and $45,000 UYU on groceries last month").

## 12. Security Requirements

| Requirement | Description |
|-------------|-------------|
| **S1 — User isolation** | Every query is scoped to a `user_id`. The service injects user filtering using the mapped `user_id` column (direct or via `ColumnRef` + JOIN). Tables marked `user_scoped=False` (e.g., shared category tables) are not filtered. The LLM never controls user scoping. |
| **S2 — No credential exposure** | The `database_url` is resolved from Secrets Manager at runtime. The service never logs or transmits connection strings. |
| **S3 — Read-only** | No tool, including the fallback, can modify data. Enforced at connection level (read-only DB role, the security boundary) and service level (keyword rejection, defense-in-depth). |
| **S4 — Input sanitization** | Tool parameters are validated via Pydantic models. All queries use parameterized values (`$1`, `$2`, etc.). The fallback tool validates generated SQL structure before execution. |
| **S5 — PII in LLM context** | Transaction descriptions sent to the LLM may contain PII (merchant names, amounts). The consuming application is responsible for PII handling policy via the `pre_llm_hook`. |

## 13. Observability Requirements

| Requirement | Description |
|-------------|-------------|
| **O1 — Structured logging** | All tool invocations logged with: tool name, parameters, execution time, result row count, success/failure. Uses Python `logging` — no proprietary logging. |
| **O2 — Unresolved query log** | Failed queries logged separately for coverage analysis (see R10). |
| **O3 — Tracing hooks** | `on_tool_call` callback fires after each tool execution with a `ToolCallEvent`. Synchronous, fire-and-forget, exceptions swallowed. The consuming application bridges this to Langfuse/OpenTelemetry/etc. |
| **O4 — Cost tracking** | `AgentResponse.token_usage` contains input/output token counts from the LLM call. |

## 14. Repository Structure

```
finance-query-agent/
├── src/
│   └── finance_query_agent/
│       ├── __init__.py               <- Package exports: SchemaMapping, exceptions, etc.
│       ├── agent.py                  <- Pydantic AI agent definition
│       ├── query_builder.py          <- Generates parameterized SQL from SchemaMapping + tool params
│       ├── connection.py             <- asyncpg single connection (Lambda-aware)
│       ├── tools/
│       │   ├── __init__.py
│       │   ├── unified.py            <- query_expenses, query_income, query_balance_history (view-backed)
│       │   ├── transactions.py       <- search_transactions
│       │   ├── recurring.py          <- get_recurring_expenses (query + Python post-processing)
│       │   └── fallback_sql.py       <- Constrained SQL generation tool
│       ├── validation/
│       │   ├── __init__.py
│       │   ├── sql_validator.py      <- Keyword rejection, table/column allowlist, LIMIT injection
│       │   └── schema_validator.py   <- Validates SchemaMapping against live DB on startup
│       └── schemas/
│           ├── __init__.py
│           ├── mapping.py            <- SchemaMapping, TableMapping, ViewMapping, JoinDef, ColumnRef
│           ├── unified_results.py    <- ExpenseGroup, IncomeMonth, BalanceSnapshot
│           ├── tool_results.py       <- Transaction, TransactionSearchResult, RecurringExpense
│           └── responses.py          <- AgentResponse, ToolCallRecord, TokenUsage
├── tests/
│   ├── test_query_builder.py         <- Unit tests for SQL generation from mappings
│   ├── test_validation/
│   ├── test_tools/
│   ├── test_agent.py
│   └── conftest.py                   <- Fixtures with test Postgres (via testcontainers or similar)
├── examples/
│   ├── basic_usage.py
│   └── fastapi_integration.py
├── pyproject.toml
├── LICENSE                           <- MIT
└── README.md
```

## 15. Integration with my_personal_incomes_ai

The consuming app deploys the finance-query-agent as an AWS Lambda via the Terraform module, providing the SchemaMapping as JSON. MPI's backend invokes the agent Lambda directly via `boto3.client('lambda').invoke()`.

**Terraform integration:**

```hcl
module "finance_agent" {
  source = "../finance-query-agent/terraform"

  schema_config_json = file("${path.module}/agent_schema.json")
  ecr_image_uri      = "${module.finance_agent.ecr_repository_url}:latest"
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

## 16. Schema Mapping Versioning

The `SchemaMapping` model is part of the service's configuration API. Changes to it follow semver:

- **Patch:** Bug fixes, no mapping changes.
- **Minor:** New optional fields on `SchemaMapping`/`TableMapping` (backward compatible). New optional column keys. New tools that activate when optional columns are mapped.
- **Major:** New required fields, renamed fields, removed fields, changed semantics of existing fields.

## 17. Design Decisions & Clarifications

### 17.1 `JoinDef.on` Format

The `on` field accepts a single equality condition in the form `table.column = table.column`. Compound join conditions (AND) are not supported in v1. If a join requires multiple conditions, use a single equality on the primary key and filter additional conditions in the WHERE clause.

### 17.2 Sign-Based `AmountConvention` Aggregation

When `sign_means_expense="negative"`, expense amounts are stored as negative values (e.g., -50.00). Spending tools use `SUM(ABS(amount))` to produce positive totals. When `sign_means_expense="positive"`, expense amounts are positive, and spending tools use `SUM(amount)` directly. In both cases, filtering for expenses uses the sign: `WHERE amount < 0` (negative convention) or `WHERE amount > 0` (positive convention). Income filtering uses the opposite sign.

### 17.3 UNION ALL with Independent `AmountConvention`

When primary and secondary transaction tables have different `AmountConvention` settings, the query builder applies each table's convention independently within its side of the `UNION ALL`. Each SELECT applies the correct filtering and aggregation for its own convention before the UNION.

### 17.4 Single Connection Model

The service uses a single `asyncpg.connect()` per Lambda invocation instead of a connection pool. This matches Lambda's execution model (one concurrent request per instance). The connection is created at request start and closed in a `finally` block. DB credentials are resolved from Secrets Manager on cold start and cached via `lru_cache`.

### 17.5 Description as Merchant Identity

In v1, merchant grouping uses the raw `description` column value. "NETFLIX.COM 03/01" and "Netflix Inc" are treated as separate merchants. Merchant normalization (fuzzy matching, alias resolution) is explicitly out of scope for v1. Consumers can pre-normalize descriptions in their database if needed.

### 17.6 Recurring Expense Normalization

The `get_recurring_expenses` tool normalizes descriptions with `LOWER(TRIM(description))` only. This is intentional for v1 — it catches exact duplicates with case/whitespace variance but does not attempt fuzzy matching. Same limitation as 17.5.

### 17.7 `on_tool_call` Hook Semantics

The `on_tool_call` hook fires once per final tool execution. When the fallback SQL tool retries (via `ModelRetry`), the hook fires only on the final attempt (whether successful or the last failed attempt). Intermediate retry attempts do not trigger the hook.

### 17.8 UNION ALL Sort Order

When queries combine primary and secondary transactions via `UNION ALL`, results are sorted by the transaction date column descending (`ORDER BY date DESC`) by default. Aggregation tools that GROUP BY override this with their own ordering (e.g., `ORDER BY total_amount DESC`).

### 17.9 `account_id` Type Coercion

The `account_id` parameter on tool inputs is typed as `str`. The service passes it to asyncpg as-is. asyncpg handles coercion to the database column type (UUID, integer, text) automatically via its type codec system. No explicit casting is needed.

## 18. Open Questions

1. **Currency handling:** The service returns per-currency breakdowns. Should the LLM present all currencies, or should the system prompt instruct it to highlight the "primary" currency? If so, how is primary currency determined?
