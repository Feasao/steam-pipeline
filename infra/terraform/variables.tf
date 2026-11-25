variable "aws_region" {
  description = "Region of the raw bucket and the state bucket."
  type        = string
  default     = "eu-central-1"
}

variable "raw_bucket_name" {
  description = "Name of the raw bucket."
  type        = string
}

variable "databricks_uc_master_role_arn" {
  description = "ARN of Databricks' Unity Catalog master role from the storage credential setup."
  type        = string
}

variable "databricks_external_id" {
  description = "External id of the Databricks storage credential."
  type        = string
  sensitive   = true
}