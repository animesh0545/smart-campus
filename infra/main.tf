locals {
  name          = var.project
  git_repo_url  = "https://github.com/${var.github_repo}.git"
  app_namespace = "smart-campus"
  # Logical storage nodes for consistent hashing (requirement f).
  # node-0..2 start in the ring; node-3 is the spare that joins live in the demo.
  partition_nodes = ["node-0", "node-1", "node-2", "node-3"]
  initial_nodes   = ["node-0", "node-1", "node-2"]
}

data "aws_caller_identity" "current" {}

# ------------------------------------------------------------------ network
module "network" {
  source = "./modules/network"
  name   = local.name
  region = var.region
}

# ------------------------------------------------------------------ EKS
module "eks" {
  source             = "./modules/eks"
  name               = local.name
  kubernetes_version = var.eks_version
  vpc_id             = module.network.vpc_id
  public_subnet_ids  = module.network.public_subnet_ids
  node_instance_type = var.node_instance_type
  node_desired_size  = var.node_desired_size
  extra_admin_arns   = var.extra_admin_arns
  app_namespace      = local.app_namespace
  app_policy_json    = module.data.app_policy_json
}

# ------------------------------------------------------------------ CI/CD (ECR + GitHub OIDC)
module "cicd" {
  source                      = "./modules/cicd"
  name                        = local.name
  github_repo                 = var.github_repo
  create_github_oidc_provider = var.create_github_oidc_provider
  artifacts_bucket_arn        = module.data.artifacts_bucket_arn
}

# ------------------------------------------------------------------ storage + queues (g, b)
module "data" {
  source             = "./modules/data"
  name               = local.name
  vpc_id             = module.network.vpc_id
  private_subnet_ids = module.network.private_subnet_ids
  eks_node_sg_id     = module.eks.node_security_group_id
  partition_nodes    = local.partition_nodes
  notification_email = var.notification_email
}

# ------------------------------------------------------------------ Kafka (MSK)
module "msk" {
  count              = var.enable_msk ? 1 : 0
  source             = "./modules/msk"
  name               = local.name
  vpc_id             = module.network.vpc_id
  vpc_cidr           = module.network.vpc_cidr
  private_subnet_ids = module.network.private_subnet_ids
}

# ------------------------------------------------------------------ observability (CloudWatch groups, Grafana IAM, budget)
module "observability" {
  source           = "./modules/observability"
  name             = local.name
  cluster_name     = module.eks.cluster_name
  budget_email     = var.budget_email
  budget_limit_usd = var.budget_limit_usd
}

# ------------------------------------------------------------------ Flink on EMR (e)
module "emr" {
  count             = var.enable_emr ? 1 : 0
  source            = "./modules/emr"
  name              = local.name
  vpc_id            = module.network.vpc_id
  vpc_cidr          = module.network.vpc_cidr
  subnet_id         = module.network.public_subnet_ids[0]
  release_label     = var.emr_release
  instance_type     = var.emr_instance_type
  artifacts_bucket  = module.data.artifacts_bucket
  flink_log_group   = module.observability.flink_log_group
}

# ------------------------------------------------------------------ Notification Lambda (b)
module "lambda" {
  source               = "./modules/lambda"
  name                 = local.name
  source_dir           = "${path.root}/../services/notification-lambda"
  queue_arn            = module.data.notifications_queue_arn
  uploads_bucket       = module.data.uploads_bucket
  uploads_bucket_arn   = module.data.uploads_bucket_arn
  notifications_table  = module.data.notifications_table
  notifications_arn    = module.data.notifications_table_arn
  sns_topic_arn        = module.data.sns_topic_arn
}

# ------------------------------------------------------------------ ArgoCD + cluster bootstrap config (d)
module "argocd" {
  source        = "./modules/argocd"
  depends_on    = [module.eks]
  app_namespace = local.app_namespace
  git_repo_url  = local.git_repo_url
  git_revision  = var.git_revision
  aws_region    = var.region

  platform_config = {
    AWS_REGION            = var.region
    AWS_DEFAULT_REGION    = var.region
    KAFKA_BOOTSTRAP       = var.enable_msk ? module.msk[0].bootstrap_brokers : ""
    KAFKA_REPLICATION     = "2"
    BOOKING_EVENTS_TOPIC  = "booking-events"
    ANALYTICS_TOPIC       = "analytics-results"
    SQS_QUEUE_URL         = module.data.notifications_queue_url
    UPLOADS_BUCKET        = module.data.uploads_bucket
    NODE_TABLES           = jsonencode(module.data.partition_tables)
    INITIAL_NODES         = join(",", local.initial_nodes)
    ANALYTICS_TABLE       = module.data.analytics_table
    NOTIFICATIONS_TABLE   = module.data.notifications_table
    DB_HOST               = module.data.db_host
    DB_PORT               = "5432"
    DB_USER               = module.data.db_username
    ZK_HOSTS              = "zookeeper.${local.app_namespace}.svc.cluster.local:2181"
  }

  platform_secrets = {
    DB_PASSWORD = module.data.db_password
  }
}
