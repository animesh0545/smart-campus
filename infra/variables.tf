variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Name prefix for every resource"
  type        = string
  default     = "smart-campus"
}

variable "github_repo" {
  description = "GitHub repository in OWNER/NAME form. ArgoCD tracks it and GitHub Actions deploys from it."
  type        = string
}

variable "git_revision" {
  description = "Branch ArgoCD tracks"
  type        = string
  default     = "main"
}

variable "budget_email" {
  description = "Email that receives AWS Budgets alerts"
  type        = string
}

variable "budget_limit_usd" {
  description = "Monthly cost budget in USD"
  type        = number
  default     = 40
}

variable "notification_email" {
  description = "Optional email subscribed to the booking-notifications SNS topic (leave empty to skip)"
  type        = string
  default     = ""
}

# ---- cost switches: MSK and EMR are separately appliable ----
variable "enable_msk" {
  description = "Create the MSK Kafka cluster (~$0.09/hr, 20-30 min to create)"
  type        = bool
  default     = true
}

variable "enable_emr" {
  description = "Create the EMR Flink cluster (~$0.48/hr). Turn on only to test (e) and for the demo."
  type        = bool
  default     = false
}

# ---- EKS ----
variable "eks_version" {
  description = "Kubernetes version for EKS (1.35 is in standard support until Mar 2027)"
  type        = string
  default     = "1.35"
}

variable "node_instance_type" {
  type    = string
  default = "t3.large"
}

variable "node_desired_size" {
  type    = number
  default = 3
}

variable "extra_admin_arns" {
  description = "IAM user/role ARNs (e.g. the teammate's IAM user) that also get cluster-admin on EKS"
  type        = list(string)
  default     = []
}

# ---- CI ----
variable "create_github_oidc_provider" {
  description = "Set false if the account already has the token.actions.githubusercontent.com OIDC provider"
  type        = bool
  default     = true
}

# ---- EMR ----
variable "emr_release" {
  description = "EMR release; emr-7.2.0 ships Flink 1.18.1 (the version the job is built against)"
  type        = string
  default     = "emr-7.2.0"
}

variable "emr_instance_type" {
  type    = string
  default = "m5.xlarge"
}
