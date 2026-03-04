#!/usr/bin/env bash
set -euo pipefail

# bootstrap.sh — One-time setup for finance-query-agent AWS resources.
# Sources credentials from the main app's .env, creates Secrets Manager secrets,
# and prints instructions for DB user creation and GitHub configuration.

MAIN_APP_ENV="../my_personal_incomes_ai/.env"
PROJECT="finance-query-agent"
REGION="us-east-1"

echo "=== Finance Query Agent Bootstrap ==="
echo

# Source main app's .env for shared credentials
if [ ! -f "$MAIN_APP_ENV" ]; then
  echo "ERROR: $MAIN_APP_ENV not found."
  echo "Expected my_personal_incomes_ai repo alongside this one."
  exit 1
fi

# shellcheck source=/dev/null
source "$MAIN_APP_ENV"

if [ -z "${OPENAI_API_KEY:-}" ]; then
  echo "ERROR: OPENAI_API_KEY not found in $MAIN_APP_ENV"
  exit 1
fi

if [ -z "${POSTGRES_HOST:-}" ]; then
  echo "ERROR: POSTGRES_HOST not found in $MAIN_APP_ENV"
  exit 1
fi

# Generate Fernet encryption key
echo "Generating Fernet encryption key..."
ENCRYPTION_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")

# Prompt for read-only DB password
echo
read -rsp "Enter password for read-only PostgreSQL user (finance_agent_ro): " DB_PASSWORD
echo

# Collect DB credentials as JSON (jq ensures proper escaping)
DB_CREDENTIALS=$(jq -n \
  --arg host "$POSTGRES_HOST" \
  --arg dbname "${POSTGRES_DB:-personal_incomes}" \
  --arg password "$DB_PASSWORD" \
  '{host: $host, port: 5432, dbname: $dbname, username: "finance_agent_ro", password: $password}'
)

echo
echo "Populating Secrets Manager secrets..."
echo "(Secrets must already exist — created by 'terraform apply')"
echo

populate_secret() {
  local name="$1"
  local value="$2"

  echo "  Updating: $name"
  aws secretsmanager put-secret-value \
    --secret-id "$name" \
    --secret-string "$value" \
    --region "$REGION" > /dev/null
}

populate_secret "$PROJECT/db-credentials" "$DB_CREDENTIALS"
populate_secret "$PROJECT/encryption-key" "$ENCRYPTION_KEY"
populate_secret "$PROJECT/llm-api-key" "$OPENAI_API_KEY"

if [ -n "${LOGFIRE_TOKEN:-}" ]; then
  populate_secret "$PROJECT/logfire-token" "$LOGFIRE_TOKEN"
else
  echo "  Skipping: $PROJECT/logfire-token (LOGFIRE_TOKEN not set)"
fi

echo
echo "=== Secrets populated ==="
echo
echo "--- Next Steps ---"
echo
echo "1) Create read-only PostgreSQL user. Connect to your RDS and run:"
echo
cat <<'SQL'
  CREATE USER finance_agent_ro WITH PASSWORD '<the password you just entered>';
  GRANT CONNECT ON DATABASE personal_incomes TO finance_agent_ro;
  GRANT USAGE ON SCHEMA public TO finance_agent_ro;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO finance_agent_ro;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO finance_agent_ro;
SQL
echo
echo "2) Configure GitHub repository secrets and variables:"
echo
echo "  gh secret set AWS_ACCESS_KEY_ID --body '<your-aws-access-key>'"
echo "  gh secret set AWS_SECRET_ACCESS_KEY --body '<your-aws-secret-key>'"
echo "  gh secret set SCHEMA_CONFIG_JSON --body '<your-schema-mapping-json>'"
echo "  gh variable set ALLOWED_ORIGINS --body '[\"https://your-frontend.cloudfront.net\"]'"
echo
echo "3) Push to main to trigger the deploy pipeline."
echo
echo "=== Done ==="
