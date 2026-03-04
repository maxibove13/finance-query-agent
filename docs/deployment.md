# Deployment Guide

## Prerequisites

- AWS account with permissions for Lambda, DynamoDB, ECR, Secrets Manager
- Terraform >= 1.5
- Docker
- A publicly accessible RDS PostgreSQL instance (no VPC required)

## Setup Flow

1. Configure GitHub secrets/variables (see below)
2. Merge to `main` -- deploy pipeline creates all AWS resources (ECR, Lambda, DynamoDB, Function URL, Secrets Manager shells)
3. Run `scripts/bootstrap.sh` to populate secret values
4. Create the read-only PostgreSQL user (SQL printed by bootstrap)

## GitHub Configuration

**Secrets:**
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `SCHEMA_CONFIG_JSON` -- your SchemaMapping JSON

**Variables:**
- `ALLOWED_ORIGINS` -- HCL list, e.g. `["https://d1234.cloudfront.net"]`

## Terraform

The `terraform/` directory is self-contained. The deploy pipeline runs `terraform apply` automatically on merge to `main`. Secrets Manager secrets are created by Terraform (empty shells), then populated by `scripts/bootstrap.sh`.

Required Terraform variables (passed via `TF_VAR_*` in CI):
- `schema_config_json` -- SchemaMapping JSON
- `allowed_origins` -- list of frontend origins
- `ecr_image_uri` -- set dynamically by CI

## Secrets

Four secrets are managed in Secrets Manager (created by Terraform, populated by bootstrap):

1. **DB credentials** -- JSON with `username`, `password`, `host`, `port`, `dbname` for the read-only PostgreSQL role
2. **Encryption key** -- Fernet key for DynamoDB field encryption
3. **LLM API key** -- OpenAI API key
4. **Logfire token** (optional) -- for observability

## Backend Integration

The Function URL uses `AWS_IAM` authorization. Callers must sign requests with SigV4. This is intended for backend-to-backend calls, not direct browser access.

```env
AGENT_FUNCTION_URL=<function_url from terraform output>
```

### Python (boto3)

```python
import json
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
import botocore.session
import requests

session = botocore.session.get_session()
credentials = session.get_credentials()

body = json.dumps({
    "user_id": "user-123",
    "session_id": "session-abc",
    "question": "How much did I spend on groceries last month?"
})

request = AWSRequest(method="POST", url=AGENT_FUNCTION_URL, data=body,
                     headers={"Content-Type": "application/json"})
SigV4Auth(credentials, "lambda", "us-east-1").add_auth(request)

response = requests.post(request.url, headers=dict(request.headers), data=body)
```

### Node.js (aws4)

```javascript
const aws4 = require("aws4");
const https = require("https");

const body = JSON.stringify({
  user_id: "user-123",
  session_id: "session-abc",
  question: "How much did I spend on groceries last month?"
});

const opts = aws4.sign({
  host: new URL(process.env.AGENT_FUNCTION_URL).host,
  path: "/",
  method: "POST",
  body,
  service: "lambda",
  region: "us-east-1",
  headers: { "Content-Type": "application/json" },
});

// Use opts.headers with your HTTP client (fetch, axios, etc.)
```

### Granting Access

Add caller IAM role ARNs to the `authorized_caller_arns` Terraform variable:

```hcl
module "finance_agent" {
  # ...
  authorized_caller_arns = [
    "arn:aws:iam::123456789012:role/my-backend-role",
  ]
}
```
