variable "bucket_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}

resource "aws_s3_bucket" "checkpoints" {
  bucket = var.bucket_name
  tags = merge(var.tags, {
    Name        = var.bucket_name
    Environment = var.environment
  })
}

resource "aws_s3_bucket_versioning" "checkpoints" {
  bucket = aws_s3_bucket.checkpoints.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "checkpoints" {
  bucket = aws_s3_bucket.checkpoints.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "checkpoints" {
  bucket                  = aws_s3_bucket.checkpoints.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

output "bucket_name" {
  value = aws_s3_bucket.checkpoints.id
}

output "bucket_arn" {
  value = aws_s3_bucket.checkpoints.arn
}
