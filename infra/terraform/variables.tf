variable "aws_region" {
  description = "Region of the raw bucket and the state bucket."
  type        = string
  default     = "eu-central-1"
}

variable "raw_bucket_name" {
  description = "Name of the raw bucket."
  type        = string
}