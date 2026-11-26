output "bootstrap_brokers_sasl_iam" {
  description = "MSK Serverless bootstrap endpoint (IAM)."
  value       = aws_msk_serverless_cluster.this.bootstrap_brokers_sasl_iam
}

output "load_balancer_dns_name" {
  description = "Internal DNS name of the API."
  value       = aws_lb.this.dns_name
}

output "task_role_arn" {
  value = aws_iam_role.task.arn
}
