# LocalStack Lambda Testing

Deploy and test the finance-query-agent Lambda locally using LocalStack.

## Instructions

### Step 1: Check prerequisites

1. Verify `.env.localstack` exists in the project root. If not, tell the user:
   > Copy `.env.localstack.example` to `.env.localstack` and set `DATABASE_URL` and `OPENAI_API_KEY`.
2. Verify Docker is running: `docker info > /dev/null 2>&1`

### Step 2: Deploy

Run the setup script:

```bash
bash localstack/setup.sh
```

This script:
- Starts LocalStack via Docker Compose (`localstack/docker-compose.yml`)
- Creates the DynamoDB conversations table
- Builds the Lambda zip inside a Linux Docker container (required — macOS-built C extensions like asyncpg/pydantic_core won't work)
- Uploads to LocalStack S3 (zip exceeds 50MB direct upload limit)
- Generates a fresh Fernet encryption key (old DynamoDB data is purged because it's undecryptable with the new key)
- Creates the Lambda function

### Step 3: Test

**IMPORTANT: Always use dummy creds.** Your default AWS profile's SSO token will interfere with LocalStack CLI calls, even with `--endpoint-url`. Override explicitly:

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url=http://localhost:4566 lambda invoke \
    --function-name finance-query-agent \
    --cli-binary-format raw-in-base64-out \
    --payload '{"user_id": 1, "session_id": "test", "question": "How much did I spend last month?"}' \
    /dev/stdout 2>/dev/null | python3 -m json.tool
```

Run at least 2 invocations to verify warm starts work (the second reuses the Lambda container).

### Step 4: Debug failures

If the invocation returns an error:

1. **Find the Lambda container** (name changes on each redeploy):
   ```bash
   docker ps --filter "name=lambda-finance" --format "{{.Names}}"
   ```

2. **Check container logs** (Lambda stdout/stderr goes here, not LocalStack main container):
   ```bash
   docker logs $(docker ps --filter "name=lambda-finance" --format "{{.Names}}") 2>&1 | tail -50
   ```
   Note: The main `localstack-localstack-1` container only shows HTTP request logs (DynamoDB, S3, Lambda invoke status codes). Actual Lambda errors (tracebacks, print output) are in the Lambda sidecar container.

3. **Common errors:**
   - `Event loop is closed` — Handler uses a persistent event loop; if this appears, the handler fix may have regressed. Check `handler.py` for `asyncio.run()` (should use `_get_event_loop().run_until_complete()` instead).
   - `InvalidToken` from Fernet — Stale DynamoDB data encrypted with a previous key. Re-run `setup.sh` (it purges old data).
   - `user_id must be an integer` — The test DB uses integer user IDs. Use `"user_id": 1`, not `"user_id": "1"`.
   - Schema validation errors — `schema-config.json` doesn't match the target database. Update `localstack/schema-config.json`.
   - `array schema missing items` from OpenAI — A Pydantic model uses `tuple[X, Y]` which generates `prefixItems` in JSON Schema. OpenAI requires `items`. Fix: use `list[X]` with `Field(min_length=N, max_length=N)` instead.
   - `Token has expired` / SSO errors — You forgot to override AWS creds. Always prefix commands with `AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1`.

### Step 5: Teardown

```bash
bash localstack/teardown.sh
```

This stops LocalStack and removes the `localstack/volume` directory. Docker Compose `down -v` alone doesn't clean host bind mounts.

## Gotchas

- **`QUERY_MODEL` not `LLM_MODEL`**: The app reads from `Settings.query_model`. Terraform and the setup script must use `QUERY_MODEL`.
- **SSO token interference**: If your default AWS profile uses SSO, expired tokens can break LocalStack CLI calls even with dummy creds. The setup script exports `AWS_ACCESS_KEY_ID=test` to override.
- **Zip must be < 250MB unzipped**: The setup script strips unused pydantic-ai provider deps (temporalio, cohere, anthropic, mistral, etc.) to stay under the limit.
- **`--cli-binary-format raw-in-base64-out`**: Required for AWS CLI v2 Lambda invoke payloads.
