# ECR repositories (one per containerised microservice) and the IAM role that
# GitHub Actions assumes through OIDC, so no long-lived AWS keys sit in GitHub.

variable "name" { type = string }
variable "github_repo" { type = string }
variable "create_github_oidc_provider" { type = bool }
variable "artifacts_bucket_arn" { type = string }

locals {
  images = [
    "user-service",
    "resource-registry",
    "room-booking",
    "equipment-booking",
    "availability",
    "partition-manager",
    "usage-analytics",
  ]
}

resource "aws_ecr_repository" "svc" {
  for_each             = toset(local.images)
  name                 = "${var.name}/${each.key}"
  image_tag_mutability = "MUTABLE"
  force_delete         = true # lets `terraform destroy` remove repos that still hold images

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "svc" {
  for_each   = aws_ecr_repository.svc
  repository = each.value.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep the last 15 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 15 }
      action       = { type = "expire" }
    }]
  })
}

# ---------------- GitHub Actions OIDC ----------------
resource "aws_iam_openid_connect_provider" "github" {
  count           = var.create_github_oidc_provider ? 1 : 0
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1", "1c58a3a8518e8759bf075b76b750d4f2df264fcd"]
}

data "aws_caller_identity" "current" {}

locals {
  oidc_provider_arn = var.create_github_oidc_provider ? aws_iam_openid_connect_provider.github[0].arn : "arn:aws:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/token.actions.githubusercontent.com"
}

data "aws_iam_policy_document" "gha_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [local.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repo}:*"]
    }
  }
}

resource "aws_iam_role" "gha" {
  name               = "${var.name}-github-actions"
  assume_role_policy = data.aws_iam_policy_document.gha_trust.json
}

data "aws_iam_policy_document" "gha" {
  statement {
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }
  statement {
    actions = [
      "ecr:BatchCheckLayerAvailability", "ecr:BatchGetImage", "ecr:CompleteLayerUpload",
      "ecr:GetDownloadUrlForLayer", "ecr:InitiateLayerUpload", "ecr:PutImage", "ecr:UploadLayerPart",
      "ecr:DescribeRepositories",
    ]
    resources = [for r in aws_ecr_repository.svc : r.arn]
  }
  statement {
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${var.artifacts_bucket_arn}/flink/*"]
  }
}

resource "aws_iam_role_policy" "gha" {
  name   = "${var.name}-github-actions"
  role   = aws_iam_role.gha.id
  policy = data.aws_iam_policy_document.gha.json
}

output "repository_urls" { value = { for k, r in aws_ecr_repository.svc : k => r.repository_url } }
output "github_actions_role_arn" { value = aws_iam_role.gha.arn }
