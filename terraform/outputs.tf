output "function_url" {
  value = aws_lambda_function_url.agent.function_url
}

output "dynamodb_table_name" {
  value = aws_dynamodb_table.conversations.name
}

output "ecr_repository_url" {
  value = data.aws_ecr_repository.agent.repository_url
}

output "lambda_function_name" {
  value = aws_lambda_function.agent.function_name
}
