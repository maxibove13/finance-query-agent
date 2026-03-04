data "aws_region" "current" {}

# ECR Repository
resource "aws_ecr_repository" "agent" {
  name                 = var.project_name
  image_tag_mutability = "MUTABLE"
  force_delete         = false

  image_scanning_configuration {
    scan_on_push = true
  }
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

# VPC access for RDS
resource "aws_iam_role_policy_attachment" "lambda_vpc" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
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
        Resource = compact([
          var.db_credentials_secret_arn,
          var.encryption_key_secret_arn,
          var.llm_api_key_secret_arn,
          var.logfire_token_secret_arn,
        ])
      },
    ]
  })
}

# Security Group for Lambda
resource "aws_security_group" "lambda" {
  name_prefix = "${var.project_name}-lambda-"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-lambda"
  }
}

# Allow Lambda to reach RDS
resource "aws_security_group_rule" "rds_ingress_from_lambda" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.lambda.id
  security_group_id        = var.rds_security_group_id
}

# Lambda Function
resource "aws_lambda_function" "agent" {
  function_name = var.project_name
  role          = aws_iam_role.lambda.arn
  package_type  = "Image"
  image_uri     = var.ecr_image_uri
  memory_size   = var.memory_size
  timeout       = var.timeout

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      DYNAMODB_TABLE              = aws_dynamodb_table.conversations.name
      DYNAMODB_REGION             = data.aws_region.current.name
      LLM_MODEL                   = var.llm_model
      SCHEMA_CONFIG_JSON          = var.schema_config_json
      DB_CREDENTIALS_SECRET_ARN   = var.db_credentials_secret_arn
      ENCRYPTION_KEY_SECRET_ARN   = var.encryption_key_secret_arn
      LLM_API_KEY_SECRET_ARN      = var.llm_api_key_secret_arn
      LOGFIRE_TOKEN_SECRET_ARN    = var.logfire_token_secret_arn
    }
  }

  image_config {
    command = ["finance_query_agent.handler.handler"]
  }

  tags = {
    Project = var.project_name
  }
}
