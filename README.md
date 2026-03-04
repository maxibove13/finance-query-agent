# finance-query-agent

AI-powered natural language query agent for financial databases. Deployed as an AWS Lambda behind a Function URL with IAM auth. Uses Pydantic AI with predefined parameterized query tools and a constrained SQL fallback.

## Architecture

```
Client (SigV4) -> Function URL -> Agent Lambda
                                   ├── asyncpg -> RDS (read-only)
                                   ├── Pydantic AI -> LLM API
                                   ├── DynamoDB (encrypted conversation history)
                                   └── Logfire (PII-scrubbed traces)
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
  "token_usage": {"input_tokens": 1200, "output_tokens": 85}
}
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
