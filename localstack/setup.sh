#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$PROJECT_DIR/.env.localstack"
ENDPOINT="http://localhost:4566"
FUNCTION_NAME="finance-query-agent"
TABLE_NAME="finance-agent-conversations"
REGION="us-east-1"

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=$REGION

# ── 1. Load env file ────────────────────────────────────────────────────────

if [ ! -f "$ENV_FILE" ]; then
  echo "ERROR: $ENV_FILE not found."
  echo "Copy .env.localstack.example to .env.localstack and fill in your values."
  exit 1
fi

set -a
source "$ENV_FILE"
set +a

: "${DATABASE_URL:?DATABASE_URL must be set in .env.localstack}"
: "${OPENAI_API_KEY:?OPENAI_API_KEY must be set in .env.localstack}"

# ── 2. Start LocalStack ─────────────────────────────────────────────────────

echo "Starting LocalStack..."
docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d

echo -n "Waiting for LocalStack to be ready"
for i in $(seq 1 30); do
  if curl -sf "$ENDPOINT/_localstack/health" > /dev/null 2>&1; then
    echo " ready."
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo " timed out after 30s."
    exit 1
  fi
  echo -n "."
  sleep 1
done

# ── 3. Create DynamoDB table ────────────────────────────────────────────────

echo "Creating DynamoDB table: $TABLE_NAME"
aws --endpoint-url="$ENDPOINT" dynamodb create-table \
  --table-name "$TABLE_NAME" \
  --key-schema \
    AttributeName=PK,KeyType=HASH \
    AttributeName=SK,KeyType=RANGE \
  --attribute-definitions \
    AttributeName=PK,AttributeType=S \
    AttributeName=SK,AttributeType=S \
    AttributeName=user_id,AttributeType=S \
    AttributeName=updated_at,AttributeType=S \
  --global-secondary-indexes \
    'IndexName=user_id-index,KeySchema=[{AttributeName=user_id,KeyType=HASH},{AttributeName=updated_at,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
  --billing-mode PAY_PER_REQUEST \
  --region "$REGION" \
  > /dev/null 2>&1 || echo "  (table already exists, skipping)"

# ── 4. Build Lambda zip ─────────────────────────────────────────────────────

echo "Building Lambda deployment package (in Docker)..."
BUILD_DIR=$(mktemp -d)
trap 'rm -rf "$BUILD_DIR"' EXIT

# Export requirements from lock file
uv export --no-dev --frozen --no-emit-project -o "$BUILD_DIR/requirements.txt" \
  --project "$PROJECT_DIR" --quiet

# Build zip inside a Linux container matching Lambda runtime
docker run --rm \
  --entrypoint bash \
  --platform linux/amd64 \
  -v "$PROJECT_DIR":/src:ro \
  -v "$BUILD_DIR":/build \
  public.ecr.aws/lambda/python:3.12 \
  -c '
    pip install -q -t /build/package -r /build/requirements.txt 2>&1 | tail -1
    pip install -q -t /build/package --no-deps /src 2>&1 | tail -1

    # Remove unused LLM provider deps pulled in by pydantic-ai meta-package.
    # Only openai + logfire extras are needed; everything else is dead weight.
    cd /build/package
    REMOVE=(
      temporalio cohere anthropic mistral groq xai_sdk
      huggingface_hub tokenizers hf_xet fastavro
      ag_ui_protocol fasta2a fastmcp
    )
    for pkg in "${REMOVE[@]}"; do
      rm -rf "$pkg" "$pkg"_* "${pkg}.dist-info" "${pkg}-"*.dist-info 2>/dev/null || true
    done

    python3 -c "
import zipfile, os
with zipfile.ZipFile(\"/build/lambda.zip\", \"w\", zipfile.ZIP_DEFLATED) as zf:
    for root, dirs, files in os.walk(\"/build/package\"):
        for f in files:
            full = os.path.join(root, f)
            zf.write(full, os.path.relpath(full, \"/build/package\"))
"
  '

ZIP_PATH="$BUILD_DIR/lambda.zip"
ZIP_SIZE=$(du -h "$ZIP_PATH" | cut -f1)
echo "  Package size: $ZIP_SIZE"

# Upload to S3 (zip exceeds 50MB direct upload limit)
BUCKET="lambda-deploy"
aws --endpoint-url="$ENDPOINT" s3 mb "s3://$BUCKET" --region "$REGION" 2>/dev/null || true
aws --endpoint-url="$ENDPOINT" s3 cp "$ZIP_PATH" "s3://$BUCKET/lambda.zip" --region "$REGION" --quiet

# ── 5. Generate Fernet key ──────────────────────────────────────────────────

ENCRYPTION_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")

# ── 6. Build environment JSON ───────────────────────────────────────────────

# Minify schema JSON to single line (no newlines inside the env var value)
SCHEMA_JSON=$(python3 -c "import json,sys; print(json.dumps(json.load(sys.stdin), separators=(',',':')))" < "$SCRIPT_DIR/schema-config.json")

ENV_JSON=$(python3 -c "
import json, sys
env = {
    'Variables': {
        'DATABASE_URL': sys.argv[1],
        'SCHEMA_CONFIG_JSON': sys.argv[2],
        'OPENAI_API_KEY': sys.argv[3],
        'QUERY_MODEL': sys.argv[4],
        'DYNAMODB_TABLE': sys.argv[5],
        'DYNAMODB_REGION': sys.argv[6],
        'ENCRYPTION_KEY': sys.argv[7],
        'AWS_ENDPOINT_URL': 'http://host.docker.internal:4566',
        'AWS_ACCESS_KEY_ID': 'test',
        'AWS_SECRET_ACCESS_KEY': 'test',
    }
}
print(json.dumps(env))
" "$DATABASE_URL" "$SCHEMA_JSON" "$OPENAI_API_KEY" "${QUERY_MODEL:-openai:gpt-4o}" "$TABLE_NAME" "$REGION" "$ENCRYPTION_KEY")

# ── 7. Create Lambda function ───────────────────────────────────────────────

echo "Deploying Lambda function: $FUNCTION_NAME"

# Write env JSON to temp file (avoids shell escaping issues)
ENV_FILE_TMP="$BUILD_DIR/env.json"
echo "$ENV_JSON" > "$ENV_FILE_TMP"

# Check if function already exists
if aws --endpoint-url="$ENDPOINT" lambda get-function \
    --function-name "$FUNCTION_NAME" --region "$REGION" > /dev/null 2>&1; then
  echo "  Function exists, deleting and recreating..."

  # Kill the hot Lambda container so the new deploy gets a fresh cold start.
  # LocalStack reuses containers, and asyncio.run() leaves the event loop closed.
  LAMBDA_CID=$(docker ps -q --filter "name=lambda-${FUNCTION_NAME}" 2>/dev/null || true)
  if [ -n "$LAMBDA_CID" ]; then
    docker rm -f "$LAMBDA_CID" > /dev/null 2>&1 || true
  fi

  aws --endpoint-url="$ENDPOINT" lambda delete-function \
    --function-name "$FUNCTION_NAME" \
    --region "$REGION" \
    > /dev/null

  # Purge DynamoDB conversations — the Fernet key is regenerated each deploy,
  # so old encrypted data is undecryptable and causes InvalidToken errors.
  echo "  Purging stale conversation data from DynamoDB..."
  ITEMS=$(aws --endpoint-url="$ENDPOINT" dynamodb scan \
    --table-name "$TABLE_NAME" \
    --projection-expression "PK,SK" \
    --region "$REGION" \
    --output json 2>/dev/null | python3 -c "
import json, sys
items = json.load(sys.stdin).get('Items', [])
for item in items:
    print(json.dumps({'PK': item['PK'], 'SK': item['SK']}))
" 2>/dev/null || true)
  if [ -n "$ITEMS" ]; then
    echo "$ITEMS" | while read -r key; do
      aws --endpoint-url="$ENDPOINT" dynamodb delete-item \
        --table-name "$TABLE_NAME" \
        --key "$key" \
        --region "$REGION" > /dev/null 2>&1 || true
    done
  fi
fi

aws --endpoint-url="$ENDPOINT" lambda create-function \
  --function-name "$FUNCTION_NAME" \
  --runtime python3.12 \
  --handler finance_query_agent.handler.handler \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --code S3Bucket="$BUCKET",S3Key=lambda.zip \
  --timeout 30 \
  --memory-size 512 \
  --environment "file://$ENV_FILE_TMP" \
  --region "$REGION" \
  > /dev/null

# ── 8. Wait for function to be Active ───────────────────────────────────────

echo -n "Waiting for function to become Active"
for i in $(seq 1 30); do
  STATE=$(aws --endpoint-url="$ENDPOINT" lambda get-function \
    --function-name "$FUNCTION_NAME" \
    --query 'Configuration.State' --output text \
    --region "$REGION" 2>/dev/null || echo "Pending")
  if [ "$STATE" = "Active" ]; then
    echo " active."
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo " timed out."
    exit 1
  fi
  echo -n "."
  sleep 1
done

# ── 9. Done ─────────────────────────────────────────────────────────────────

echo ""
echo "LocalStack is ready."
echo ""
echo "Test with:"
echo "  aws --endpoint-url=$ENDPOINT lambda invoke \\"
echo "    --function-name $FUNCTION_NAME \\"
echo "    --cli-binary-format raw-in-base64-out \\"
echo "    --payload '{\"user_id\":\"1\",\"session_id\":\"test\",\"question\":\"How much did I spend in January 2026?\"}' \\"
echo "    /dev/stdout"
echo ""
echo "Tear down with:"
echo "  ./localstack/teardown.sh"
