resource "aws_s3_bucket" "raw" {
  bucket = var.raw_bucket_name

  lifecycle {
    prevent_destroy = true
  }
}

import {
  to = aws_s3_bucket.raw
  id = var.raw_bucket_name
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id

  versioning_configuration {
    status = "Enabled"
  }

  lifecycle {
    prevent_destroy = true
  }
}

import {
  to = aws_s3_bucket_versioning.raw
  id = var.raw_bucket_name
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket = aws_s3_bucket.raw.id

  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true

  lifecycle {
    prevent_destroy = true
  }
}

import {
  to = aws_s3_bucket_public_access_block.raw
  id = var.raw_bucket_name
}