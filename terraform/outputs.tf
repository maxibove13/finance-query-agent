output "lambda_function_name" {
  value = aws_lambda_function.agent.function_name
}

output "lambda_function_arn" {
  value = aws_lambda_function.agent.arn
}

output "dynamodb_table_name" {
  value = aws_dynamodb_table.conversations.name
}

output "ecr_repository_url" {
  value = data.aws_ecr_repository.agent.repository_url
}

output "schema_config_ssm_parameter_name" {
  value = aws_ssm_parameter.schema_config.name
}
