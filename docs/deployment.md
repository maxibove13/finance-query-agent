# Deployment Guide

## Prerequisites

- AWS account with permissions for Lambda, DynamoDB, ECR, Secrets Manager
- Terraform >= 1.5
- Docker
- A publicly accessible RDS PostgreSQL instance (no VPC required)

## Setup Flow

1. Configure GitHub secrets (see below)
2. Merge to `main` -- deploy pipeline creates all AWS resources (ECR, Lambda, DynamoDB, Secrets Manager shells)
3. Run `scripts/bootstrap.sh` to populate secret values
4. Create the read-only PostgreSQL user (SQL printed by bootstrap)

## GitHub Configuration

**Secrets:**
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

## Terraform

The `terraform/` directory is self-contained. The deploy pipeline runs `terraform apply` automatically on merge to `main`. Secrets Manager secrets are created by Terraform (empty shells), then populated by `scripts/bootstrap.sh`.

Required Terraform variables (passed via `TF_VAR_*` in CI):
- `ecr_image_uri` -- set dynamically by CI

## Schema Config

The `SchemaMapping` JSON is stored in SSM Parameter Store at `/<project-name>/schema-config`. The SSM parameter is created by Terraform on first deploy. After that, the client's CI/CD pipeline (e.g. MPI) is responsible for updating it via `aws ssm put-parameter --overwrite`.

**Important:** The Lambda reads SSM once per cold start (cached via `get_settings()`). After updating the parameter, force a cold start so the Lambda picks up the new config:

```bash
aws lambda update-function-configuration \
  --function-name finance-query-agent \
  --description "schema config updated $(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

If the new schema config doesn't match the live database, the Lambda returns `503 schema_mismatch` -- the client should treat this as a non-retryable config error.

## Secrets

Four secrets are managed in Secrets Manager (created by Terraform, populated by bootstrap):

1. **DB credentials** -- JSON with `username`, `password`, `host`, `port`, `dbname` for the read-only PostgreSQL role
2. **Encryption key** -- Fernet key for DynamoDB field encryption
3. **LLM API key** -- OpenAI API key
4. **Logfire token** (optional) -- for observability

## Integration

The Lambda is invoked by MPI's backend via `boto3.client('lambda').invoke()`. MPI owns authentication and user identity. This project only provides the Lambda.

Terraform outputs for MPI's integration:
- `lambda_function_name` -- for `boto3.client('lambda').invoke(FunctionName=...)`
- `lambda_function_arn` -- for IAM permissions (`lambda:InvokeFunction`)

The caller wraps the payload in `event["body"]`:

```python
import json
import boto3

lambda_client = boto3.client("lambda")
payload = {"user_id": "user-123", "session_id": "session-abc", "question": "..."}
response = lambda_client.invoke(
    FunctionName="finance-query-agent",
    Payload=json.dumps({"body": json.dumps(payload)}),
)
result = json.loads(response["Payload"].read())
answer = json.loads(result["body"])
```
