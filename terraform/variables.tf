variable "project_name" {
  type    = string
  default = "finance-query-agent"
}

# LLM
variable "llm_model" {
  type    = string
  default = "openai:gpt-4o"
}

# Schema
variable "schema_config_json" {
  type        = string
  description = "SchemaMapping JSON configuration"
}

# CORS
variable "allowed_origins" {
  type        = list(string)
  description = "Origins allowed to call the Function URL (frontend domains)"
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
