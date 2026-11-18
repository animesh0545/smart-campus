output "region" {
  value = var.region
}

output "cluster_name" {
  value = module.eks.cluster_name
}

output "kubeconfig_command" {
  value = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}

output "ecr_registry" {
  value = "${data.aws_caller_identity.current.account_id}.dkr.ecr.${var.region}.amazonaws.com"
}

output "ecr_repositories" {
  value = module.cicd.repository_urls
}

output "github_actions_role_arn" {
  description = "Put this in the GitHub repo variable AWS_ROLE_ARN"
  value       = module.cicd.github_actions_role_arn
}

output "artifacts_bucket" {
  value = module.data.artifacts_bucket
}

output "uploads_bucket" {
  value = module.data.uploads_bucket
}

output "rds_endpoint" {
  value = module.data.db_host
}

output "partition_tables" {
  value = module.data.partition_tables
}

output "notifications_table" {
  value = module.data.notifications_table
}

output "analytics_table" {
  value = module.data.analytics_table
}

output "notifications_queue_url" {
  value = module.data.notifications_queue_url
}

output "lambda_function_name" {
  value = module.lambda.function_name
}

output "msk_bootstrap_brokers" {
  value = var.enable_msk ? module.msk[0].bootstrap_brokers : ""
}

output "emr_cluster_id" {
  value = var.enable_emr ? module.emr[0].cluster_id : ""
}

output "flink_log_group" {
  value = module.observability.flink_log_group
}
