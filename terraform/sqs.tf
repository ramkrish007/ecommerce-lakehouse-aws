# Quarantine queue (acts as our dead-letter queue for bad records)
resource "aws_sqs_queue" "quarantine" {
  name                      = "${var.project_name}-quarantine"
  message_retention_seconds = 1209600 # 14 days (max) — keep bad records around to inspect
  sqs_managed_sse_enabled   = true    # encrypt messages at rest
}