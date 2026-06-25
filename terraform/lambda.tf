# --- IAM role the Lambda assumes when it runs ---
resource "aws_iam_role" "lambda_validate" {
  name = "${var.project_name}-lambda-validate-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

# Permissions: read bronze, write to the quarantine queue, write CloudWatch logs
resource "aws_iam_role_policy" "lambda_validate_policy" {
  name = "${var.project_name}-lambda-validate-policy"
  role = aws_iam_role.lambda_validate.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${aws_s3_bucket.bronze.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = aws_sqs_queue.quarantine.arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# --- Package the Lambda code into a zip ---
data "archive_file" "validate_zip" {
  type        = "zip"
  source_file = "${path.module}/../lambda/validate_schema.py"
  output_path = "${path.module}/build/validate_schema.zip"
}

# --- The Lambda function itself ---
resource "aws_lambda_function" "validate" {
  function_name = "${var.project_name}-validate-schema"
  role          = aws_iam_role.lambda_validate.arn
  handler       = "validate_schema.lambda_handler"
  runtime       = "python3.12"
  timeout       = 60
  memory_size   = 256

  filename         = data.archive_file.validate_zip.output_path
  source_code_hash = data.archive_file.validate_zip.output_base64sha256

  environment {
    variables = {
      DLQ_URL = aws_sqs_queue.quarantine.url
    }
  }
}