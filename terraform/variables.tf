variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "ap-south-1"
}

variable "project_name" {
  description = "Project name used as prefix for all resources"
  type        = string
  default     = "ecom-lakehouse"
}

variable "environment" {
  description = "Environment tag"
  type        = string
  default     = "dev"
}
