# MPI: Semantic Model S3 Setup

## Context

The `finance-query-agent` Lambda reads a YAML semantic model (database schema metadata) at cold start to build the LLM's system prompt. The model is stored in S3 — MPI owns the bucket because the semantic model describes **MPI's database schema**, and updates should happen alongside schema changes.

The agent reads the model via `schema_builder.py` → `s3.get_object()`, configured by `SEMANTIC_MODEL_S3_BUCKET` and `SEMANTIC_MODEL_S3_KEY` env vars.

## What MPI needs to do

### 1. Create the S3 bucket in MPI's Terraform

Add to MPI's Terraform (e.g., in a new `semantic-model.tf` or alongside existing S3 resources):

```hcl
resource "aws_s3_bucket" "semantic_model" {
  bucket = "mpi-finance-agent-semantic-model"

  tags = {
    Project = "my-personal-incomes"
  }
}

resource "aws_s3_bucket_versioning" "semantic_model" {
  bucket = aws_s3_bucket.semantic_model.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "semantic_model" {
  bucket = aws_s3_bucket.semantic_model.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
```

### 2. Store the semantic model YAML in the MPI repo

Place the semantic model YAML in the MPI repo (e.g., `config/semantic-model.yaml`). This file describes tables, columns, metrics, relationships, filters, and business rules that the agent uses to understand the database.

A reference version lives at `finance-query-agent/localstack/semantic-model.yaml` for local development. Going forward, MPI owns this file and updates it when the schema changes.

### 3. Upload the YAML to S3

Add a step to MPI's CI/CD pipeline (or a Terraform `aws_s3_object` resource) to upload the YAML on changes:

**Option A — Terraform managed (simpler):**
```hcl
resource "aws_s3_object" "semantic_model" {
  bucket       = aws_s3_bucket.semantic_model.id
  key          = "semantic-model.yaml"
  source       = "${path.module}/../config/semantic-model.yaml"
  etag         = filemd5("${path.module}/../config/semantic-model.yaml")
  content_type = "application/x-yaml"
}
```

**Option B — CI upload (more flexible):**
```bash
aws s3 cp config/semantic-model.yaml s3://mpi-finance-agent-semantic-model/semantic-model.yaml
```

### 4. Export bucket name and ARN as Terraform outputs

The agent's Terraform needs the bucket ARN for IAM policy and the bucket name for the Lambda env var:

```hcl
output "semantic_model_bucket_name" {
  value = aws_s3_bucket.semantic_model.id
}

output "semantic_model_bucket_arn" {
  value = aws_s3_bucket.semantic_model.arn
}
```

### 5. Grant the agent Lambda read access

Either:

**Option A — Cross-stack reference:** The agent's Terraform reads MPI's outputs (via `terraform_remote_state` or SSM parameter) and adds `s3:GetObject` to its own IAM policy. Preferred if the two Terraform states are already linked.

**Option B — Bucket policy (simpler):** Add a bucket policy in MPI's Terraform granting the agent Lambda's role read access:

```hcl
data "aws_iam_role" "agent_lambda" {
  name = "finance-query-agent-lambda"
}

resource "aws_s3_bucket_policy" "semantic_model" {
  bucket = aws_s3_bucket.semantic_model.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        AWS = data.aws_iam_role.agent_lambda.arn
      }
      Action   = "s3:GetObject"
      Resource = "${aws_s3_bucket.semantic_model.arn}/semantic-model.yaml"
    }]
  })
}
```

## Agent-side configuration

The agent is already configured to read from S3. The relevant settings in `config.py`:

| Env var | Default | Description |
|---------|---------|-------------|
| `SEMANTIC_MODEL_S3_BUCKET` | *(required)* | S3 bucket name |
| `SEMANTIC_MODEL_S3_KEY` | `semantic-model.yaml` | Object key within the bucket |

The model is fetched once per cold start by `schema_builder.py` and cached for the Lambda instance's lifetime.

## Notes

- S3 is only hit on cold start (~1 read per cold start, negligible cost)
- S3 versioning is enabled so rollback is trivial — just restore a previous version
- The `localstack/semantic-model.yaml` in the agent repo is for local development only
- After updating the YAML in S3, force a cold start so the Lambda picks up the new model:
  ```bash
  aws lambda update-function-configuration \
    --function-name finance-query-agent \
    --description "schema config updated $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  ```
