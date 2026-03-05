data "aws_region" "current" {}

# ECR Repository (created by CI workflow before Terraform runs)
data "aws_ecr_repository" "agent" {
  name = var.project_name
}

# IAM Role for Lambda
resource "aws_iam_role" "lambda" {
  name = "${var.project_name}-lambda"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

# CloudWatch Logs
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# DynamoDB + Secrets Manager access
resource "aws_iam_role_policy" "lambda_app" {
  name = "${var.project_name}-app"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:Query",
        ]
        Resource = [
          aws_dynamodb_table.conversations.arn,
          "${aws_dynamodb_table.conversations.arn}/index/*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
        ]
        Resource = [
          aws_secretsmanager_secret.db_credentials.arn,
          aws_secretsmanager_secret.encryption_key.arn,
          aws_secretsmanager_secret.llm_api_key.arn,
          aws_secretsmanager_secret.logfire_token.arn,
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "ssm:GetParameter",
        ]
        Resource = [
          aws_ssm_parameter.schema_config.arn,
        ]
      },
    ]
  })
}

# Secrets Manager
resource "aws_secretsmanager_secret" "db_credentials" {
  name = "${var.project_name}/db-credentials"
}

resource "aws_secretsmanager_secret" "encryption_key" {
  name = "${var.project_name}/encryption-key"
}

resource "aws_secretsmanager_secret" "llm_api_key" {
  name = "${var.project_name}/llm-api-key"
}

resource "aws_secretsmanager_secret" "logfire_token" {
  name = "${var.project_name}/logfire-token"
}

# Schema config — seeded by Terraform, updated by client (MPI) CI/CD
resource "aws_ssm_parameter" "schema_config" {
  name  = "/${var.project_name}/schema-config"
  type  = "String"
  value = var.schema_config_json

  lifecycle {
    ignore_changes = [value]
  }
}

# Lambda Function
resource "aws_lambda_function" "agent" {
  function_name = var.project_name
  role          = aws_iam_role.lambda.arn
  package_type  = "Image"
  image_uri     = var.ecr_image_uri
  memory_size   = var.memory_size
  timeout       = var.timeout

  environment {
    variables = {
      DYNAMODB_TABLE            = aws_dynamodb_table.conversations.name
      DYNAMODB_REGION           = data.aws_region.current.name
      LLM_MODEL                 = var.llm_model
      SCHEMA_CONFIG_SSM_PARAM   = aws_ssm_parameter.schema_config.name
      DB_CREDENTIALS_SECRET_ARN = aws_secretsmanager_secret.db_credentials.arn
      ENCRYPTION_KEY_SECRET_ARN = aws_secretsmanager_secret.encryption_key.arn
      LLM_API_KEY_SECRET_ARN    = aws_secretsmanager_secret.llm_api_key.arn
      LOGFIRE_TOKEN_SECRET_ARN  = aws_secretsmanager_secret.logfire_token.arn
    }
  }

  image_config {
    command = ["finance_query_agent.handler.handler"]
  }

  tags = {
    Project = var.project_name
  }
}
