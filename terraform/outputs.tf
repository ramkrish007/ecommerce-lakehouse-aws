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
