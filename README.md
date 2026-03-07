# finance-query-agent

AI-powered financial query agent. Answers natural language questions about spending, income, and transactions. Deployed as an AWS Lambda invoked by MPI's backend via `boto3 lambda.invoke()`.

Uses a **tools-as-wrappers** architecture: the LLM picks a tool and fills parameters, the service generates and executes parameterized SQL. No raw SQL from the LLM for the common case — a constrained SQL fallback covers the long tail. A secondary **visualization agent** generates chart specs from query results when the data is chartable.

```mermaid
graph LR
    Q["User Question"] --> QUERY_AGENT

    subgraph QUERY_AGENT["Query Agent (Pydantic AI)"]
        direction TB
        subgraph PREDEFINED["Predefined Tools"]
            direction LR
            V["query_expenses\nquery_income\nquery_balance_history"]
            D["search_transactions\nget_recurring_expenses"]
        end
        FB["run_constrained_query\n(SQL fallback)"]
    end

    QUERY_AGENT -->|"TextAnswer"| OUT_TEXT["Text Response"]
    QUERY_AGENT -->|"AnswerWithVisualization"| VIZ_AGENT

    subgraph VIZ_AGENT["Visualization Agent"]
        direction TB
        VIZ_IN["Chartable tool results\n(≥ 2 rows)"]
        VIZ_OUT["pie · bar · line · grouped_bar"]
    end

    VIZ_AGENT --> OUT_VIZ["Text + Chart Specs"]

    V --> MV[("Materialized Views\n(pre-computed)")]
    D --> QB["QueryBuilder\n(SchemaMapping → SQL)"]
    QB --> PG[("PostgreSQL")]
    MV --> PG
    FB --> PG

    style QUERY_AGENT fill:#2a2a3c,stroke:#88c,color:#fff
    style PREDEFINED fill:#2d5a3d,stroke:#4a9,color:#fff
    style FB fill:#5a3d2d,stroke:#a94,color:#fff
    style VIZ_AGENT fill:#3a3a5c,stroke:#88c,color:#fff
```

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
    Backend->>Lambda: boto3 lambda.invoke()

    Lambda->>Dynamo: Load conversation history
    Dynamo-->>Lambda: Encrypted messages (Fernet)

    Lambda->>LLM: System prompt + history + question
    LLM-->>Lambda: Tool call: query_expenses(...)

    Lambda->>PG: Parameterized SQL ($1, $2, ...)
    PG-->>Lambda: Query results

    Lambda->>LLM: Tool results
    LLM-->>Lambda: AnswerWithVisualization

    Note over Lambda: Viz agent runs if chartable data ≥ 2 rows
    Lambda->>LLM: Viz agent: question + tool results
    LLM-->>Lambda: ChartSpec[]

    Lambda->>Dynamo: Save updated history (encrypted)
    Lambda-->>Backend: AgentResponse JSON (answer + charts)
    Backend-->>Client: Answer + visualizations
```

## Architecture

```mermaid
graph TB
    subgraph Client["Client (MPI)"]
        FE[React Frontend]
        BE[FastAPI Backend]
    end

    FE -->|question| BE
    BE -->|boto3 invoke| HANDLER

    subgraph AWS["AWS"]
        subgraph Lambda["Lambda (30s timeout)"]
            HANDLER[handler.py<br/><i>Entry point</i>]
            HANDLER --> AGENT
            AGENT[Query Agent<br/><i>agent.py</i>]
            AGENT --> TOOLS
            AGENT --> FALLBACK

            subgraph TOOLS["Predefined Tools"]
                T1[query_expenses]
                T2[query_income]
                T3[query_balance_history]
                T4[search_transactions]
                T5[get_recurring_expenses]
            end

            FALLBACK[run_constrained_query<br/><i>SQL fallback</i>]
            QB[QueryBuilder<br/><i>SchemaMapping → SQL</i>]
            T4 --> QB
            T5 --> QB

            AGENT -->|AnswerWithVisualization| VIZ
            VIZ[Visualization Agent<br/><i>visualization.py</i>]
        end

        RDS[(RDS PostgreSQL<br/><i>read-only role</i>)]
        DDB[(DynamoDB<br/><i>conversation memory</i>)]
    end

    LLM_API[LLM API<br/><i>OpenAI</i>]
    LOGFIRE[Logfire<br/><i>PII-scrubbed traces</i>]

    QB -->|parameterized queries| RDS
    FALLBACK -->|validated SQL| RDS
    HANDLER <-->|encrypted history| DDB
    AGENT <-->|inference| LLM_API
    VIZ <-->|inference| LLM_API
    HANDLER -.->|traces| LOGFIRE

    style TOOLS fill:#2d5a3d,stroke:#4a9,color:#fff
    style FALLBACK fill:#5a3d2d,stroke:#a94,color:#fff
    style VIZ fill:#3a3a5c,stroke:#88c,color:#fff
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
        V["View-Backed<br/>─────────────<br/>query_expenses<br/>query_income<br/>query_balance_history"]
        D["Direct Query<br/>─────────────<br/>search_transactions<br/>get_recurring_expenses"]
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

    V -->|"$1, $2, ..."| DB[(PostgreSQL)]
    D --> QB[QueryBuilder]
    QB -->|"$1, $2, ..."| DB
    CONSTRAINED -->|"validated SQL"| DB

    style PREDEFINED fill:#2d5a3d,stroke:#4a9,color:#fff
    style CONSTRAINED fill:#5a3d2d,stroke:#a94,color:#fff
```

## Query Generation Pipeline

View-backed tools (`query_expenses`, `query_income`, `query_balance_history`) query pre-computed materialized views directly. Direct query tools (`search_transactions`, `get_recurring_expenses`) use the `QueryBuilder` to generate SQL from `SchemaMapping`.

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

    GQ -->|connection pool| PG[(PostgreSQL)]
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
        IAM["MPI Backend<br/><i>boto3 invoke + IAM role</i>"]
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
        VIEWS["unified views<br/><i>(optional)</i><br/>─────────────────<br/>unified_expenses<br/>unified_income<br/>unified_balances<br/><i>pre-computed materialized views</i>"]
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

POST request with JSON body:

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
  "visualizations": [
    {
      "chart_type": "pie",
      "title": "Spending by Category (USD)",
      "currency": "USD",
      "slices": [{"label": "Groceries", "value": 235.50, "percentage": 42.1}, ...]
    }
  ],
  "fallback_used": false,
  "unresolved": false,
  "original_question": "How much did I spend on groceries last month?",
  "token_usage": { "input_tokens": 1200, "output_tokens": 85 }
}
```

`visualizations` is `null` when the query agent returns `TextAnswer` or the data isn't chartable. Chart types: `pie`, `bar`, `line`, `grouped_bar`.

## Project Structure

```
src/finance_query_agent/
├── handler.py              Lambda entry point
├── agent.py                Query agent + system prompt
├── visualization.py        Visualization agent (chart spec generation)
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
│   ├── unified.py          query_expenses, query_income, query_balance_history (view-backed)
│   ├── transactions.py     search_transactions
│   ├── recurring.py        get_recurring_expenses
│   └── fallback_sql.py     Constrained SQL generation
├── validation/
│   ├── sql_validator.py    Keyword rejection, allowlist, LIMIT injection
│   └── schema_validator.py Validates mapping against live DB
└── schemas/
    ├── mapping.py          SchemaMapping, TableMapping, ViewMapping, JoinDef, ColumnRef
    ├── charts.py           Chart specs (pie, bar, line, grouped_bar)
    ├── unified_results.py  ExpenseGroup, IncomeMonth, BalanceSnapshot
    ├── tool_results.py     Transaction, TransactionSearchResult, RecurringExpense
    └── responses.py        AgentResponse, AgentOutput, ChartSpec
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
