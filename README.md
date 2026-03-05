# finance-query-agent

AI-powered financial query agent. Answers natural language questions about spending, income, and transactions. Deployed as an AWS Lambda behind a Function URL.

Uses a **tools-as-wrappers** architecture: the LLM picks a tool and fills parameters, the service generates and executes parameterized SQL. No raw SQL from the LLM for the common case — a constrained SQL fallback covers the long tail.

## Request Lifecycle

```mermaid
sequenceDiagram
    participant Client as MPI Frontend
    participant Backend as MPI Backend
    participant Lambda as Agent Lambda
    participant LLM as LLM API
    participant PG as PostgreSQL
    participant Dynamo as DynamoDB

    Client->>Backend: "How much did I spend on groceries?"
    Backend->>Lambda: POST (SigV4 signed)

    Lambda->>Dynamo: Load conversation history
    Dynamo-->>Lambda: Encrypted messages (Fernet)

    Lambda->>LLM: System prompt + history + question
    LLM-->>Lambda: Tool call: get_spending_by_category(...)

    Lambda->>PG: Parameterized SQL ($1, $2, ...)
    PG-->>Lambda: Query results

    Lambda->>LLM: Tool results
    LLM-->>Lambda: "You spent $235.50 on groceries"

    Lambda->>Dynamo: Save updated history (encrypted)
    Lambda-->>Backend: AgentResponse JSON
    Backend-->>Client: Answer + metadata
```

## Architecture Overview

```mermaid
graph TB
    subgraph Client["Client (MPI)"]
        FE[React Frontend]
        BE[FastAPI Backend]
    end

    FE -->|question| BE
    BE -->|SigV4 POST| FURL

    subgraph AWS["AWS"]
        FURL[Function URL<br/><i>IAM Auth</i>]
        FURL --> HANDLER

        subgraph Lambda["Lambda (15 min timeout)"]
            HANDLER[handler.py<br/><i>Entry point</i>]
            HANDLER --> AGENT
            AGENT[Pydantic AI Agent<br/><i>agent.py</i>]
            AGENT --> TOOLS
            AGENT --> FALLBACK

            subgraph TOOLS["Predefined Tools"]
                T1[get_spending_by_category]
                T2[get_monthly_totals]
                T3[get_balance_summary]
                T4[get_top_merchants]
                T5[search_transactions]
                T6[compare_periods]
                T7[get_spending_trend]
                T8[get_category_breakdown]
                T9[get_recurring_expenses]
            end

            FALLBACK[run_constrained_query<br/><i>SQL fallback</i>]

            QB[QueryBuilder<br/><i>SchemaMapping → SQL</i>]
            TOOLS --> QB
        end

        RDS[(RDS PostgreSQL<br/><i>read-only role</i>)]
        DDB[(DynamoDB<br/><i>conversation memory</i>)]
    end

    LLM_API[LLM API<br/><i>OpenAI / Anthropic</i>]
    LOGFIRE[Logfire<br/><i>PII-scrubbed traces</i>]

    QB -->|parameterized queries| RDS
    FALLBACK -->|validated SQL| RDS
    HANDLER <-->|encrypted history| DDB
    AGENT <-->|inference| LLM_API
    HANDLER -.->|traces| LOGFIRE

    style TOOLS fill:#2d5a3d,stroke:#4a9,color:#fff
    style FALLBACK fill:#5a3d2d,stroke:#a94,color:#fff
    style RDS fill:#1a3a5c,stroke:#4a9,color:#fff
    style DDB fill:#1a3a5c,stroke:#4a9,color:#fff
```

## Tool Architecture

The agent has two tiers of query tools. The LLM always prefers predefined tools — the fallback is a last resort.

```mermaid
graph LR
    Q[User Question] --> AGENT[Pydantic AI Agent]

    AGENT -->|"Tier 1 (preferred)"| PREDEFINED
    AGENT -->|"Tier 2 (fallback)"| CONSTRAINED

    subgraph PREDEFINED["Predefined Tools — Safe by Construction"]
        direction TB
        S["Spending<br/>─────────────<br/>by_category<br/>monthly_totals<br/>balance_summary"]
        X["Search<br/>─────────────<br/>transactions<br/>top_merchants"]
        R["Trends<br/>─────────────<br/>compare_periods<br/>spending_trend<br/>category_breakdown"]
        RC["Patterns<br/>─────────────<br/>recurring_expenses"]
    end

    subgraph CONSTRAINED["Constrained SQL — Validated + Sandboxed"]
        direction TB
        V1["1. Keyword rejection<br/><i>no INSERT/UPDATE/DELETE/DROP</i>"]
        V2["2. Table/column allowlist<br/><i>only mapped schema</i>"]
        V3["3. EXPLAIN validation<br/><i>syntax + reference check</i>"]
        V4["4. User ID injection<br/><i>strips LLM filters, adds own</i>"]
        V5["5. LIMIT injection<br/><i>max 200 rows</i>"]
        V1 --> V2 --> V3 --> V4 --> V5
    end

    PREDEFINED --> QB[QueryBuilder]
    QB -->|"$1, $2, ..."| DB[(PostgreSQL)]
    CONSTRAINED -->|"validated SQL"| DB

    style PREDEFINED fill:#2d5a3d,stroke:#4a9,color:#fff
    style CONSTRAINED fill:#5a3d2d,stroke:#a94,color:#fff
```

## Query Generation Pipeline

All SQL is derived from a declarative `SchemaMapping` — no hand-written queries.

```mermaid
graph LR
    SM["SchemaMapping<br/>(JSON config)"] --> QB[QueryBuilder]
    TP["Tool Parameters<br/><i>from LLM</i>"] --> QB
    UID["user_id<br/><i>injected by service</i>"] --> QB

    QB --> GQ["GeneratedQuery"]

    subgraph GQ["GeneratedQuery"]
        SQL["Parameterized SQL<br/><code>SELECT ... WHERE user_id = $1<br/>AND date >= $2 AND date <= $3</code>"]
        PARAMS["Params: ['user-123', '2026-02-01', '2026-02-28']"]
    end

    GQ -->|single connection| PG[(PostgreSQL)]
    PG --> ROWS[Result Rows]
    ROWS --> LLM[LLM formats answer]

    style SM fill:#3a3a5c,stroke:#88c,color:#fff
    style GQ fill:#1a3a5c,stroke:#4a9,color:#fff
```

For multi-source schemas (bank accounts + credit cards), the builder generates `UNION ALL` with independent JOINs per table and re-aggregates across both sources.

## Conversation Memory

```mermaid
graph TB
    subgraph Request["Each Request"]
        LOAD["1. Load history<br/><i>DynamoDB GET</i>"]
        DECRYPT["2. Decrypt<br/><i>Fernet</i>"]
        RUN["3. Agent run<br/><i>history + new question</i>"]
        SUMMARIZE["4. Summarize if long<br/><i>history_processors</i>"]
        ENCRYPT["5. Encrypt<br/><i>Fernet</i>"]
        SAVE["6. Save history<br/><i>DynamoDB PUT</i>"]

        LOAD --> DECRYPT --> RUN --> SUMMARIZE --> ENCRYPT --> SAVE
    end

    subgraph DynamoDB["DynamoDB Table"]
        direction TB
        ITEM["<b>Item</b><br/>──────────────────<br/>PK: USER#user-123<br/>SK: SESSION#sess-abc<br/>user_id: user-123<br/>messages_json: <i>(Fernet ciphertext)</i><br/>updated_at: 2026-03-05T..."]
    end

    LOAD <-.->|"asyncio.to_thread"| DynamoDB
    SAVE <-.->|"asyncio.to_thread"| DynamoDB

    style DynamoDB fill:#1a3a5c,stroke:#4a9,color:#fff
    style Request fill:#2a2a3c,stroke:#88c,color:#fff
```

## Security Model

```mermaid
graph TB
    subgraph AUTH["Authentication"]
        SIGV4["AWS SigV4<br/><i>Function URL + IAM</i>"]
    end

    subgraph ISOLATION["User Isolation"]
        INJ["Service injects user_id<br/><i>from authenticated caller</i>"]
        STRIP["Strips LLM-generated<br/>user_id conditions"]
        SCOPE["Every query scoped<br/>WHERE user_id = $1"]
        INJ --> STRIP --> SCOPE
    end

    subgraph READONLY["Read-Only Enforcement"]
        ROLE["DB role: read-only<br/><i>security boundary</i>"]
        KW["Keyword rejection<br/><i>defense in depth</i>"]
    end

    subgraph PII["PII Protection"]
        FERNET["Fernet encryption<br/><i>DynamoDB at rest</i>"]
        REGEX["Regex scrubbing<br/><i>Logfire traces</i>"]
    end

    subgraph SQLS["SQL Safety"]
        PARAM["Parameterized queries<br/><i>$1, $2 — no interpolation</i>"]
        ALLOW["Table/column allowlist<br/><i>derived from SchemaMapping</i>"]
        EXPLAIN["EXPLAIN before execute<br/><i>fallback tool only</i>"]
        TIMEOUT["30s query timeout"]
    end

    AUTH --> ISOLATION
    ISOLATION --> READONLY
    ISOLATION --> SQLS
    PII ~~~ SQLS

    style AUTH fill:#5a3d2d,stroke:#a94,color:#fff
    style ISOLATION fill:#5a3d2d,stroke:#a94,color:#fff
    style READONLY fill:#5a3d2d,stroke:#a94,color:#fff
    style PII fill:#3a3a5c,stroke:#88c,color:#fff
    style SQLS fill:#3a3a5c,stroke:#88c,color:#fff
```

## Schema Mapping (Client Integration)

The only thing a client provides. A declarative config that maps their DB schema to the agent's tools.

```mermaid
graph LR
    subgraph SchemaMapping["SchemaMapping (JSON)"]
        direction TB
        TX["transactions<br/>─────────────────<br/>table: account_movements<br/>columns: date, amount, ...<br/>joins: accounts, tags<br/>amount_convention: debit/credit"]
        CAT["categories<br/>─────────────────<br/>table: tags<br/>columns: id, name<br/>user_scoped: false"]
        ACCT["accounts<br/>─────────────────<br/>table: accounts<br/>columns: id, name, user_id"]
        SEC["secondary_transactions<br/><i>(optional)</i><br/>─────────────────<br/>table: credit_card_movements<br/>independent joins + convention"]
    end

    SchemaMapping --> DERIVES

    subgraph DERIVES["Service Derives"]
        direction TB
        D1["All predefined tool queries"]
        D2["Fallback SQL allowlist"]
        D3["User isolation WHERE clauses"]
        D4["UNION ALL for multi-source"]
        D5["Schema validation on startup"]
    end

    style SchemaMapping fill:#3a3a5c,stroke:#88c,color:#fff
    style DERIVES fill:#2d5a3d,stroke:#4a9,color:#fff
```

## Invocation

The Function URL requires AWS SigV4 authentication. Send a POST request:

```json
{
  "user_id": "user-123",
  "session_id": "sess-abc",
  "question": "How much did I spend on groceries last month?"
}
```

Response:

```json
{
  "answer": "You spent $235.50 on groceries last month across 3 transactions.",
  "tool_calls": [...],
  "fallback_used": false,
  "unresolved": false,
  "original_question": "How much did I spend on groceries last month?",
  "token_usage": { "input_tokens": 1200, "output_tokens": 85 }
}
```

## Project Structure

```
src/finance_query_agent/
├── handler.py              Lambda entry point (Function URL)
├── agent.py                Pydantic AI agent + system prompt
├── config.py               Settings from env vars
├── query_builder.py        SchemaMapping → parameterized SQL
├── connection.py           asyncpg single connection (Lambda-aware)
├── memory.py               DynamoDB conversation history
├── encryption.py           Fernet field encryption
├── redaction.py            Regex PII scrubbing
├── history.py              Conversation summarization
├── observability.py        Logfire + scrubbing callback
├── exceptions.py           Exception hierarchy
├── tools/
│   ├── spending.py         by_category, monthly_totals, balance_summary
│   ├── transactions.py     search_transactions, top_merchants
│   ├── trends.py           compare_periods, spending_trend, category_breakdown
│   ├── recurring.py        get_recurring_expenses
│   └── fallback_sql.py     Constrained SQL generation
├── validation/
│   ├── sql_validator.py    Keyword rejection, allowlist, LIMIT injection
│   └── schema_validator.py Validates mapping against live DB
└── schemas/
    ├── mapping.py          SchemaMapping, TableMapping, JoinDef, ColumnRef
    ├── tool_params.py      Tool input parameter models
    ├── tool_results.py     Tool return type models
    └── responses.py        AgentResponse, ToolCallRecord, TokenUsage
```

## Development

```bash
uv sync --all-extras              # Install all deps (including dev)
uv run pytest                     # Run all tests
uv run pytest -x                  # Stop on first failure
uv run ruff check . --fix         # Lint + auto-fix
uv run ruff format .              # Format
uv run mypy src/                  # Type check
```

## Deployment

See `docs/deployment.md` and `terraform/` for infrastructure setup.

## License

MIT
