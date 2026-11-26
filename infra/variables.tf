variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "name" {
  description = "Name prefix for every resource."
  type        = string
  default     = "fraudstream"
}

variable "vpc_id" {
  description = "VPC that hosts MSK, the load balancer and the tasks."
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnets (at least two AZs) for MSK Serverless and the ECS tasks."
  type        = list(string)

  validation {
    condition     = length(var.private_subnet_ids) >= 2
    error_message = "Provide at least two subnets in different availability zones."
  }
}

variable "container_image" {
  description = "Image built with --build-arg MAVEN_PROFILES=msk, e.g. an ECR URI with a tag."
  type        = string
}

variable "auth_tokens_secret_arn" {
  description = "Secrets Manager secret whose value is the FRAUDSTREAM_AUTH_TOKENS string (name:ROLE:sha256 entries)."
  type        = string
}

variable "allowed_cidr_blocks" {
  description = "CIDR ranges allowed to reach the internal load balancer."
  type        = list(string)
}

variable "desired_count" {
  description = "Number of tasks; Kafka Streams spreads partitions across them."
  type        = number
  default     = 2
}

variable "task_cpu" {
  type    = number
  default = 1024
}

variable "task_memory" {
  type    = number
  default = 2048
}

variable "grace" {
  description = "Event-time grace period (ISO-8601 duration)."
  type        = string
  default     = "PT5M"
}

variable "log_retention_days" {
  type    = number
  default = 30
}
