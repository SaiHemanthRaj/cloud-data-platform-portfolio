variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "bucket_name" {
  type        = string
  description = "Globally unique S3 bucket name for raw portfolio data"
}
