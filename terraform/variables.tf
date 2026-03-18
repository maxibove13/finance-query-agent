variable "project_name" {
  type    = string
  default = "finance-query-agent"
}

# LLM
variable "primary_model" {
  type    = string
  default = "openai:gpt-4.1"
}

variable "secondary_model" {
  type    = string
  default = "openai:gpt-4.1-mini"
}

# Lambda
variable "memory_size" {
  type    = number
  default = 1024
}

variable "timeout" {
  type    = number
  default = 30
}

variable "ecr_image_uri" {
  type = string
}

# Function URL
variable "allowed_origins" {
  type    = list(string)
  default = []
}

variable "authorized_caller_arns" {
  type    = list(string)
  default = []
}

# Semantic model
variable "semantic_model_s3_bucket" {
  type        = string
  description = "S3 bucket containing the semantic model YAML"
}

variable "semantic_model_s3_key" {
  type    = string
  default = "semantic-model.yaml"
}
