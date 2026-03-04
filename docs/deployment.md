# Deployment Guide

## Prerequisites

- AWS account with permissions for Lambda, DynamoDB, ECR, VPC, Secrets Manager
- Terraform >= 1.0
- Docker
- An existing VPC with private subnets and RDS instance

## Terraform Module

The `terraform/` directory is a self-contained module. Consume it from your infrastructure repo:

```hcl
module "finance_agent" {
  source = "../finance-query-agent/terraform"

  vpc_id                    = module.vpc.vpc_id
  subnet_ids                = module.vpc.private_subnets
  rds_security_group_id     = aws_security_group.rds.id
  rds_endpoint              = aws_db_instance.main.endpoint
  db_credentials_secret_arn = aws_secretsmanager_secret.agent_readonly_db.arn
  encryption_key_secret_arn = aws_secretsmanager_secret.fernet_key.arn
  llm_api_key_secret_arn    = aws_secretsmanager_secret.openai_key.arn
  allowed_origins           = ["https://your-frontend-domain.com"]
  schema_config_json        = file("${path.module}/agent_schema.json")
  ecr_image_uri             = "${module.finance_agent.ecr_repository_url}:latest"
}
```

## Build & Deploy

```bash
# 1. Build Docker image
docker build -t finance-query-agent .

# 2. Tag and push to ECR
aws ecr get-login-password | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
docker tag finance-query-agent:latest <ecr-repo-url>:latest
docker push <ecr-repo-url>:latest

# 3. Apply Terraform
cd terraform && terraform apply

# 4. Update Lambda to use new image
aws lambda update-function-code \
  --function-name finance-query-agent \
  --image-uri <ecr-repo-url>:latest
```

## Secrets Setup

Create these in Secrets Manager before deploying:

1. **DB credentials** — JSON secret with `username`, `password`, `host`, `port`, `dbname` for the read-only PostgreSQL role
2. **Encryption key** — Fernet key for DynamoDB field encryption (generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`)
3. **LLM API key** — OpenAI API key as `OPENAI_API_KEY`
4. **Logfire token** (optional) — for observability

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
