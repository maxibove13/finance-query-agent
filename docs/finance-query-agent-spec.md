# Finance Query Agent SDK — Specification Requirements

## 1. Context & Motivation

This SDK originates from [my_personal_incomes_ai](https://github.com/facusorg/my_personal_incomes_ai), a personal finance application that processes bank statements (PDF/CSV), uses AI to extract and categorize transactions, and displays spending analytics. The stack is FastAPI + PostgreSQL on the backend, React + TypeScript on the frontend, with Pydantic AI (OpenAI + Mistral) powering the parsing pipeline.

The app already handles the full import flow — upload, text extraction, AI parsing, keyword-based categorization — and stores structured transaction data across multiple tables (`account_movements`, `credit_card_movements`, `tags`, `accounts`, etc.). What it lacks is a way for users to **ask questions about their data in natural language**.

Rather than building this capability as a tightly coupled feature inside the app, we're extracting it into a standalone, open-source SDK. This serves two purposes:

1. **For the app**: adds a differentiating, monetizable feature — an AI financial assistant that answers spending questions with reliable, auditable results.
2. **For the community**: provides a reusable library that any application with a financial database can integrate by providing a schema mapping, without writing query logic.

The SDK is designed as the first consumer's needs dictate (our app's schema, our data model's quirks), but generic enough that other financial applications can adopt it.

## 2. Problem Statement

Users of financial applications need to ask natural language questions about their data ("How much did I spend on groceries last month?", "Compare my spending this month vs last month"). Building this as raw text-to-SQL is unreliable — wrong JOINs, hallucinated column names, and plausible-but-incorrect results erode trust. A tools-based agent with predefined, parameterized query operations provides reliability for the common case, while a constrained SQL fallback covers the long tail.

## 3. Goals

- Provide a **public, pip-installable Python SDK** (`finance-query-agent`) that any application with a financial database can integrate.
- Use **Pydantic AI** as the agent framework.
- Implement a **tools-as-wrappers** architecture: the LLM selects which tool to call and with what parameters; the tool executes a predefined, parameterized query.
- Include a **constrained SQL generation tool** as a fallback for queries not covered by predefined tools.
- **Configuration-driven integration**: clients provide a declarative schema mapping (table names, column names, joins). The SDK generates all queries internally. No adapter code to write.

## 4. Non-Goals

- Not a hosted service. The SDK is a library consumed by application code.
- Not a BI/analytics platform. No dashboards, no visualizations, no semantic layer.
- No write operations. The agent is strictly read-only.
- No multi-database support in v1. PostgreSQL only. The schema mapping approach allows future database backends.
- No conversation memory in v1. Each query is stateless. Memory can be added by the consuming application or in a future version.
- No custom tool overrides or extension points. The SDK provides a fixed set of tools. If a question can't be answered by those tools, the constrained SQL fallback handles it.

## 5. Architecture

```
┌──────────────────────────────────────────────────────┐
│  Consuming Application (e.g. my_personal_incomes_ai) │
│                                                      │
│  ┌───────────────┐    ┌───────────────────────────┐  │
│  │ API endpoint  │───>│ SchemaMapping config      │  │
│  │ (chat/query)  │    │ (table names, columns,    │  │
│  └───────────────┘    │  joins — declarative)     │  │
│                       └─────────────┬─────────────┘  │
└─────────────────────────────────────┼────────────────┘
                                      │
┌─────────────────────────────────────┼────────────────┐
│  finance-query-agent SDK            │                │
│                                     ▼                │
│  ┌──────────────────────────────────────────────┐    │
│  │  Query Builder (generates parameterized SQL  │    │
│  │  from SchemaMapping + tool parameters)       │    │
│  └──────────────────────┬───────────────────────┘    │
│                         │                            │
│  ┌──────────────────────▼───────────────────────┐    │
│  │  Pydantic AI Agent                           │    │
│  │                                              │    │
│  │  Tools:                                      │    │
│  │  ├── get_spending_by_category                │    │
│  │  ├── get_monthly_totals                      │    │
│  │  ├── get_top_merchants                       │    │
│  │  ├── compare_periods                         │    │
│  │  ├── search_transactions                     │    │
│  │  ├── get_category_breakdown                  │    │
│  │  ├── get_spending_trend                      │    │
│  │  ├── get_balance_summary                     │    │
│  │  ├── get_recurring_expenses                  │    │
│  │  └── [fallback] run_constrained_query        │    │
│  └──────────────────────────────────────────────┘    │
│                         │                            │
│  ┌──────────────────────▼───────────────────────┐    │
│  │  PostgreSQL (asyncpg, read-only connection)  │    │
│  └──────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────┘
```

**The SDK owns:** agent definition, tool definitions, query building, prompt engineering, response formatting, SQL validation, database connection management.

**The consuming app owns:** schema mapping configuration, API exposure, authentication, rate limiting.

## 6. Schema Mapping (Client Integration)

This is the only thing a client needs to provide. A declarative configuration that tells the SDK where financial data lives in their database.

### 6.1 Configuration Model

```python
from finance_query_agent import (
    create_agent,
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

The SDK resolves `ColumnRef` by finding the matching `JoinDef` in the table's `joins` list. If no join to the referenced table exists, schema validation fails at startup.

### 6.3 `AmountConvention` — Expense vs. Income

Financial databases represent transaction direction differently. The SDK supports two conventions:

```python
class AmountConvention:
    # Option A: separate direction column (e.g. CREDIT/DEBIT enum)
    direction_column: str | None = None
    expense_value: str | None = None
    income_value: str | None = None

    # Option B: sign-based (positive = expense or positive = income)
    sign_means_expense: Literal["positive", "negative"] | None = None
```

Exactly one of the two options must be set. The SDK uses this to generate the correct `WHERE` clause when filtering for expenses (all spending tools) or income. When a tool needs "total spending," the SDK filters to expenses only. When a tool needs "all transactions" (e.g., `search_transactions`), no direction filter is applied.

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
| Balance | `balance` | `numeric` | Running balance after transaction (enables `get_balance_summary`) |

### 6.6 `user_scoped` Flag

Tables that are shared/global (no `user_id` column) MUST set `user_scoped=False`. Default is `True`. The SDK will NOT inject user filtering on tables marked `user_scoped=False`. User isolation on transaction queries comes from the `user_id` mapping on the transactions table (whether direct or via `ColumnRef`).

### 6.7 What the SDK Derives from the Mapping

| SDK gets | From |
|----------|------|
| All predefined tool queries | Column mappings + join definitions + amount convention |
| Expense/income filtering | `AmountConvention` on each transaction table |
| Fallback SQL table/column allowlist | All mapped tables and columns (nothing else is queryable) |
| Schema description for LLM context | Column names, types (introspected from DB), relationships |
| User isolation `WHERE` clauses | The `user_id` column mapping (direct or via `ColumnRef` + JOIN) |
| `UNION ALL` for multi-source queries | `transactions` + `secondary_transactions` with independent JOINs |

### 6.8 Schema Validation

On `create_agent()`, the SDK MUST:
1. Connect to the database and verify all mapped tables and columns exist.
2. For tables with `user_scoped=True` (default): verify the `user_id` column exists, either directly or as a `ColumnRef` with a valid join path.
3. For tables with `user_scoped=False`: skip user_id validation.
4. Verify all `JoinDef` conditions reference valid columns on both sides.
5. Verify all `ColumnRef` entries point to a table that has a corresponding `JoinDef`.
6. Verify `AmountConvention` is set on every transaction table and references valid columns.
7. If `balance` column is not mapped, disable `get_balance_summary` and log a warning.
8. Raise a clear error if any mapping is invalid, specifying exactly which table/column is wrong.

## 7. Predefined Tools

Each tool accepts typed parameters (Pydantic models) and returns structured results. The agent selects the tool and fills the parameters; the SDK's query builder generates and executes the SQL using the schema mapping.

**Note on query complexity:** Predefined tools may use CTEs, subqueries, and window functions internally. The "no subqueries" restriction (R7) applies only to the LLM-generated SQL in the fallback tool, not to the SDK's own query builder.

### 7.1 `get_spending_by_category`

Returns total spending for one or more categories within a time period. Only counts expenses (uses `AmountConvention` to filter).

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `period_start` | `date` | yes | Start of period (inclusive) |
| `period_end` | `date` | yes | End of period (inclusive) |
| `categories` | `list[str] \| None` | no | Filter to specific categories. `None` = all. |
| `account_id` | `str \| None` | no | Filter to specific account. `None` = all. |

Returns: `list[CategorySpending]` — each with `category`, `total_amount`, `transaction_count`, `currency`. Results are grouped per currency when multiple currencies exist.

### 7.2 `get_monthly_totals`

Returns aggregated expense totals per month. Only counts expenses.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `period_start` | `date` | yes | Start month |
| `period_end` | `date` | yes | End month |
| `account_id` | `str \| None` | no | Filter to specific account |

Returns: `list[MonthlyTotal]` — each with `year`, `month`, `total_amount`, `transaction_count`, `currency`. One entry per (year, month, currency) combination.

### 7.3 `get_top_merchants`

Returns the merchants where the user spends the most. Only counts expenses.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `period_start` | `date` | yes | Start of period |
| `period_end` | `date` | yes | End of period |
| `limit` | `int` | no | Number of merchants (default 10, max 50) |
| `category` | `str \| None` | no | Filter to a specific category |

Returns: `list[MerchantSpending]` — each with `merchant_name`, `total_amount`, `transaction_count`, `currency`. Ranked by `total_amount` descending within each currency.

### 7.4 `compare_periods`

Compares spending between two time periods. Only counts expenses.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `period_a_start` | `date` | yes | First period start |
| `period_a_end` | `date` | yes | First period end |
| `period_b_start` | `date` | yes | Second period start |
| `period_b_end` | `date` | yes | Second period end |
| `group_by` | `"category" \| "merchant" \| "total"` | no | Grouping (default `"total"`) |

Returns: `list[PeriodComparison]` — one per (group, currency) combination. Each with `group_label`, `currency`, `period_a_total`, `period_b_total`, `absolute_change`, `percentage_change`.

### 7.5 `search_transactions`

Searches transactions by description, amount range, date range, or category. Returns all transactions (expenses and income) unless filtered.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | `str \| None` | no | Text search on description (case-insensitive `ILIKE '%query%'`) |
| `period_start` | `date \| None` | no | Start of period |
| `period_end` | `date \| None` | no | End of period |
| `min_amount` | `float \| None` | no | Minimum absolute amount |
| `max_amount` | `float \| None` | no | Maximum absolute amount |
| `category` | `str \| None` | no | Filter by category name |
| `direction` | `"expense" \| "income" \| None` | no | Filter by direction. `None` = both. |
| `limit` | `int` | no | Max results (default 20, max 100) |
| `offset` | `int` | no | Pagination offset |

Text search uses `ILIKE '%query%'`. This is simple and works without special indexes. If the consuming app has a trigram GIN index, performance will be good on large tables; without one, scans are acceptable for the result set sizes typical of personal finance data (thousands, not millions of rows per user).

Returns: `TransactionSearchResult` — with `transactions: list[Transaction]`, `total_count`, `has_more`.

### 7.6 `get_category_breakdown`

Returns a percentage breakdown of spending by category for a period. Only counts expenses.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `period_start` | `date` | yes | Start of period |
| `period_end` | `date` | yes | End of period |
| `account_id` | `str \| None` | no | Filter to specific account |

Returns: `list[CategoryBreakdown]` — each with `category`, `total_amount`, `percentage`, `currency`. Percentages are computed within each currency independently. Uncategorized transactions (null category) appear as `category: "Uncategorized"`.

### 7.7 `get_spending_trend`

Returns spending over time for a category or total. Only counts expenses.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `period_start` | `date` | yes | Start |
| `period_end` | `date` | yes | End |
| `granularity` | `"week" \| "month"` | no | Time bucketing (default `"month"`) |
| `category` | `str \| None` | no | Filter to category. `None` = total. |

Returns: `list[TrendPoint]` — each with `period_label`, `total_amount`, `transaction_count`, `currency`.

### 7.8 `get_balance_summary`

Returns the most recent balance and activity summary per account. **Only available if `balance` column is mapped in the schema.**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `account_id` | `str \| None` | no | Specific account or all |

**Query pattern:** For each account, find the most recent transaction (by date) and return its `balance` value. Uses a window function internally:

```sql
SELECT DISTINCT ON (account_id) account_id, account_name, balance, date
FROM transactions JOIN accounts ...
WHERE user_id = $1
ORDER BY account_id, date DESC
```

Returns: `list[AccountSummary]` — each with `account_name`, `latest_balance`, `last_transaction_date`, `currency`.

If `balance` is not mapped in the schema, the SDK disables this tool (it is not registered with the agent) and logs a warning at startup.

### 7.9 `get_recurring_expenses`

Identifies recurring transactions (subscriptions, regular payments). Only counts expenses.

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

**Query pattern:** A single query with `GROUP BY description`, `COUNT(*)`, `array_agg(date ORDER BY date)`, and `percentile_cont(0.5) WITHIN GROUP (ORDER BY amount)`. Interval analysis and frequency classification happen in Python after the query returns, not in SQL.

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

**R7 — No subqueries in v1.** LLM-generated SQL must be a single `SELECT` statement. CTEs, subqueries, `DO` blocks, and multiple statements are rejected. This constraint can be relaxed in future versions after validation. (Note: this restriction applies only to the fallback tool, not to the SDK's predefined tools which use CTEs/subqueries internally.)

**R8 — User isolation injection.** The tool MUST automatically inject user scoping using the `user_id` mapping (resolving `ColumnRef` + JOINs as needed). The LLM-generated SQL MUST NOT contain any `user_id` condition — the SDK strips any LLM-generated user filtering and replaces it with its own.

**R9 — Audit logging.** Every query generated by this tool MUST be logged with: the original natural language question, the generated SQL, whether it passed validation, whether it was executed, the execution time, and the row count returned.

**R10 — Unresolved query tracking.** If the fallback tool also fails (after retries), the original question MUST be logged as an "unresolved query" for later analysis. This is the feedback loop for identifying new predefined tools to build.

## 9. Agent Configuration

### 9.1 `create_agent()` Signature

```python
from finance_query_agent import create_agent, SchemaMapping

agent = create_agent(
    # Required
    db_url: str,                          # asyncpg-compatible: "postgresql://user:pass@host/db"
    schema: SchemaMapping,
    model: str,                           # Pydantic AI model string: "openai:gpt-4o", "anthropic:claude-sonnet"

    # Fallback configuration
    fallback_enabled: bool = True,
    fallback_max_retries: int = 3,
    fallback_query_timeout_seconds: int = 30,
    fallback_result_limit: int = 200,

    # Hooks (all optional)
    pre_llm_hook: Callable[[PreLlmHookContext], PreLlmHookContext] | None = None,
    on_tool_call: Callable[[ToolCallEvent], None] | None = None,

    # Prompt
    system_prompt_override: str | None = None,
)
```

**Database URL format:** Raw `asyncpg` format: `postgresql://user:pass@host:port/dbname`. Not SQLAlchemy format (no `+asyncpg` dialect prefix). The SDK creates an `asyncpg` connection pool directly.

### 9.2 Hooks

**`pre_llm_hook`** — Called before tool results are sent back to the LLM. Use for PII redaction.

```python
class PreLlmHookContext:
    tool_name: str
    tool_results: list[dict]     # The rows about to be sent to the LLM

# Return a modified PreLlmHookContext. The SDK sends the returned version to the LLM.
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

Financial data often spans multiple currencies. The SDK handles this consistently across all tools:

- **Aggregation tools** (`get_spending_by_category`, `get_monthly_totals`, `get_top_merchants`, `compare_periods`, `get_category_breakdown`, `get_spending_trend`): Results are grouped per currency. The SDK never converts or sums across currencies. A user with USD and UYU transactions gets separate rows for each.
- **Search tools** (`search_transactions`): Return the raw currency per transaction.
- **Balance tool** (`get_balance_summary`): Returns per-account, which is inherently per-currency.
- **Recurring tool** (`get_recurring_expenses`): Groups by (description, currency) — a Netflix charge in USD and one in EUR are separate recurring items.
- **LLM formatting**: The system prompt instructs the LLM to present multi-currency results clearly (e.g., "You spent $1,200 USD and $45,000 UYU on groceries last month").

## 12. Security Requirements

| Requirement | Description |
|-------------|-------------|
| **S1 — User isolation** | Every query is scoped to a `user_id`. The SDK injects user filtering using the mapped `user_id` column (direct or via `ColumnRef` + JOIN). Tables marked `user_scoped=False` (e.g., shared category tables) are not filtered. The LLM never controls user scoping. |
| **S2 — No credential exposure** | The `db_url` is used internally to create a connection pool. The SDK never logs or transmits connection strings. |
| **S3 — Read-only** | No tool, including the fallback, can modify data. Enforced at connection level (read-only DB role, the security boundary) and SDK level (keyword rejection, defense-in-depth). |
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
finance-query-agent/                  <- Public repo (pip-installable)
├── src/
│   └── finance_query_agent/
│       ├── __init__.py               <- Public API: create_agent, SchemaMapping, etc.
│       ├── agent.py                  <- Pydantic AI agent definition
│       ├── query_builder.py          <- Generates parameterized SQL from SchemaMapping + tool params
│       ├── connection.py             <- asyncpg connection pool, read-only enforcement
│       ├── tools/
│       │   ├── __init__.py
│       │   ├── spending.py           <- get_spending_by_category, get_monthly_totals, get_balance_summary
│       │   ├── transactions.py       <- search_transactions, get_top_merchants
│       │   ├── trends.py             <- compare_periods, get_spending_trend, get_category_breakdown
│       │   ├── recurring.py          <- get_recurring_expenses (query + Python post-processing)
│       │   └── fallback_sql.py       <- Constrained SQL generation tool
│       ├── validation/
│       │   ├── __init__.py
│       │   ├── sql_validator.py      <- Keyword rejection, table/column allowlist, LIMIT injection
│       │   └── schema_validator.py   <- Validates SchemaMapping against live DB on startup
│       └── schemas/
│           ├── __init__.py
│           ├── mapping.py            <- SchemaMapping, TableMapping, JoinDef, ColumnRef, AmountConvention
│           ├── tool_params.py        <- Pydantic models for tool input parameters
│           ├── tool_results.py       <- Pydantic models for tool return types
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

The private repo adds a single configuration file and an API endpoint:

```
app/
├── services/
│   └── query_agent.py            <- SchemaMapping config + create_agent() call
├── api/
│   └── chat_endpoints.py        <- POST /api/chat — exposes the agent
```

Full integration code:

```python
# app/services/query_agent.py
from finance_query_agent import (
    create_agent, SchemaMapping, TableMapping, JoinDef, ColumnRef, AmountConvention,
)
from app.core.config import settings

schema = SchemaMapping(
    transactions=TableMapping(
        table="account_movements",
        columns={
            "date": "issued_at",
            "amount": "amount",
            "description": "description",
            "user_id": ColumnRef("accounts", "user_id"),
            "currency": ColumnRef("accounts", "currency"),
            "account_id": "account_id",
            "balance": "balance",
        },
        joins=[
            JoinDef(table="accounts", on="account_movements.account_id = accounts.id", type="inner"),
            JoinDef(table="tags", on="account_movements.category_id = tags.id", type="left"),
        ],
        amount_convention=AmountConvention(
            direction_column="movement_direction",
            expense_value="debit",
            income_value="credit",
        ),
    ),
    categories=TableMapping(
        table="tags",
        columns={"id": "id", "name": "name"},
        user_scoped=False,
    ),
    accounts=TableMapping(
        table="accounts",
        columns={"id": "id", "name": "alias", "user_id": "user_id"},
    ),
    secondary_transactions=TableMapping(
        table="credit_card_movements",
        columns={
            "date": "issued_at",
            "amount": "amount",
            "description": "description",
            "user_id": ColumnRef("credit_cards", "user_id"),
            "currency": "currency",
            "account_id": "credit_card_id",
        },
        joins=[
            JoinDef(table="credit_cards", on="credit_card_movements.credit_card_id = credit_cards.id", type="inner"),
            JoinDef(table="tags", on="credit_card_movements.category_id = tags.id", type="left"),
        ],
        amount_convention=AmountConvention(
            direction_column="movement_direction",
            expense_value="debit",
            income_value="credit",
        ),
    ),
)

query_agent = create_agent(
    db_url=settings.asyncpg_database_url,
    schema=schema,
    model="openai:gpt-4o",
)
```

```python
# app/api/chat_endpoints.py
from fastapi import APIRouter, Depends
from app.services.query_agent import query_agent
from app.api.dependencies import get_current_user

router = APIRouter()

@router.post("/api/chat")
async def chat(question: str, user=Depends(get_current_user)):
    result = await query_agent.run(question, user_id=str(user.id))
    return result
```

## 16. Schema Mapping Versioning

The `SchemaMapping` model is part of the SDK's public API. Changes to it follow semver:

- **Patch:** Bug fixes, no mapping changes.
- **Minor:** New optional fields on `SchemaMapping`/`TableMapping` (backward compatible). New optional column keys. New tools that activate when optional columns are mapped.
- **Major:** New required fields, renamed fields, removed fields, changed semantics of existing fields.

## 17. Open Questions

1. **Model choice for v1:** OpenAI GPT-4o (cheapest capable option for tool-calling) or Claude Sonnet? The SDK is model-agnostic but we need a default for testing.
2. **Currency handling:** The SDK returns per-currency breakdowns. Should the LLM present all currencies, or should the system prompt instruct it to highlight the "primary" currency? If so, how is primary currency determined?
