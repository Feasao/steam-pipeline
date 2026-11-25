# IAM for the pipeline

data "aws_caller_identity" "current" {}

locals {
  databricks_role_name = "databricks-steam-raw-read"
}

# Uploads the raw files from the VM

# The access key is not managed here
resource "aws_iam_user" "pipeline" {
  name = "steam-user"

  lifecycle {
    prevent_destroy = true
    ignore_changes  = [tags, tags_all]
  }
}

resource "aws_iam_policy" "pipeline_s3" {
  name = "steam-pipeline-s3-access"

  # List the bucket; read, write and delete only under raw/.
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = aws_s3_bucket.raw.arn
      },
      {
        Sid      = "ObjectAccess"
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = "${aws_s3_bucket.raw.arn}/raw/*"
      },
    ]
  })
}

resource "aws_iam_user_policy_attachment" "pipeline_s3" {
  user       = aws_iam_user.pipeline.name
  policy_arn = aws_iam_policy.pipeline_s3.arn
}

# Read only Databricks role with Unity Catalog storage credential

resource "aws_iam_role" "databricks_raw_read" {
  name        = local.databricks_role_name
  description = "Read perms for databricks to access the steam raw files"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "sts:AssumeRole"
      Principal = {
        AWS = [
          "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.databricks_role_name}",
          var.databricks_uc_master_role_arn,
        ]
      }
      Condition = {
        StringEquals = {
          "sts:ExternalId" = var.databricks_external_id
        }
      }
    }]
  })

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_iam_role_policy" "databricks_raw_read" {
  name = "Read-perms-databricks-steam"
  role = aws_iam_role.databricks_raw_read.name

  # Scoped to objects only under raw
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${aws_s3_bucket.raw.arn}/raw/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = aws_s3_bucket.raw.arn
      },
    ]
  })
}