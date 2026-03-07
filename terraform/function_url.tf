resource "aws_lambda_function_url" "agent" {
  function_name      = aws_lambda_function.agent.function_name
  authorization_type = "AWS_IAM"

  cors {
    allow_origins = var.allowed_origins
    allow_methods = ["POST", "OPTIONS"]
    allow_headers = ["content-type", "authorization"]
    max_age       = 3600
  }
}

resource "aws_lambda_permission" "caller" {
  for_each = toset(var.authorized_caller_arns)

  statement_id       = "AllowCaller-${md5(each.value)}"
  action             = "lambda:InvokeFunctionUrl"
  function_name      = aws_lambda_function.agent.function_name
  principal          = each.value
  function_url_auth_type = "AWS_IAM"
}
