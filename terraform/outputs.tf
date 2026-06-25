output "bronze_bucket" {
  value       = aws_s3_bucket.bronze.bucket
  description = "Bronze layer bucket name"
}

output "silver_bucket" {
  value       = aws_s3_bucket.silver.bucket
  description = "Silver layer bucket name"
}

output "gold_bucket" {
  value       = aws_s3_bucket.gold.bucket
  description = "Gold layer bucket name"
}

output "aws_region" {
  value = var.aws_region
}

output "quarantine_queue_url" {
  value       = aws_sqs_queue.quarantine.url
  description = "SQS quarantine (DLQ) queue URL"
}

output "validate_lambda_name" {
  value       = aws_lambda_function.validate.function_name
  description = "Validation Lambda function name"
}
