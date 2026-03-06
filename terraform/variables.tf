variable "project_name" {
  type    = string
  default = "finance-query-agent"
}

# LLM
variable "query_model" {
  type    = string
  default = "openai:gpt-4o"
}

# Schema
variable "schema_config_json" {
  type        = string
  sensitive   = true
  description = "SchemaMapping JSON — seeds SSM parameter on first apply, ignored after"
  default     = "{}"
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
