variable "project_name" {
  type    = string
  default = "finance-query-agent"
}

# Network — consumer provides their existing VPC
variable "vpc_id" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "rds_security_group_id" {
  type        = string
  description = "Security group of the RDS instance (agent gets ingress rule added)"
}

# Database
variable "rds_endpoint" {
  type = string
}

variable "db_credentials_secret_arn" {
  type        = string
  description = "Secrets Manager ARN for read-only DB credentials"
}

# Encryption
variable "encryption_key_secret_arn" {
  type        = string
  description = "Secrets Manager ARN for Fernet encryption key"
}

# LLM
variable "llm_api_key_secret_arn" {
  type = string
}

variable "llm_model" {
  type    = string
  default = "openai:gpt-4o"
}

# Observability
variable "logfire_token_secret_arn" {
  type    = string
  default = ""
}

# CORS
variable "allowed_origins" {
  type        = list(string)
  description = "Origins allowed to call the Function URL (frontend domains)"
}

# Schema
variable "schema_config_json" {
  type        = string
  description = "SchemaMapping JSON configuration"
}

# Lambda
variable "memory_size" {
  type    = number
  default = 1024
}

variable "timeout" {
  type    = number
  default = 120
}

variable "ecr_image_uri" {
  type = string
}

variable "authorized_caller_arns" {
  type        = list(string)
  default     = []
  description = "IAM ARNs allowed to invoke the Function URL (SigV4 auth)"
}
