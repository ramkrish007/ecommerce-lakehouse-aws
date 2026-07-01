# --- Glue Database (catalog namespace for all tables) ---
resource "aws_glue_catalog_database" "ecom_db" {
  name = "ecom_lakehouse_db"
}

# --- IAM role for Glue jobs ---
resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "glue_policy" {
  name = "${var.project_name}-glue-policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          "${aws_s3_bucket.bronze.arn}", "${aws_s3_bucket.bronze.arn}/*",
          "${aws_s3_bucket.silver.arn}", "${aws_s3_bucket.silver.arn}/*",
          "${aws_s3_bucket.gold.arn}", "${aws_s3_bucket.gold.arn}/*",
          "arn:aws:s3:::ecom-lakehouse-dc3oqp-scripts",
          "arn:aws:s3:::ecom-lakehouse-dc3oqp-scripts/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:*"]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# --- Bronze → Silver job ---
resource "aws_glue_job" "bronze_to_silver" {
  name     = "${var.project_name}-bronze-to-silver"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://ecom-lakehouse-dc3oqp-scripts/glue_jobs/bronze_to_silver.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 10 # 10 minutes max — safety cap
  max_retries       = 0  # no auto-retry for dev

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--BRONZE_BUCKET"                    = aws_s3_bucket.bronze.bucket
    "--SILVER_BUCKET"                    = aws_s3_bucket.silver.bucket
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }
}

# --- Silver → Gold job ---
resource "aws_glue_job" "silver_to_gold" {
  name     = "${var.project_name}-silver-to-gold"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://ecom-lakehouse-dc3oqp-scripts/glue_jobs/silver_to_gold.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 10
  max_retries       = 0

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--SILVER_BUCKET"                    = aws_s3_bucket.silver.bucket
    "--GOLD_BUCKET"                      = aws_s3_bucket.gold.bucket
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }
}