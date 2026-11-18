# Managed Kubernetes (requirement c) using the community module.
# Nodes sit in public subnets (no NAT); the node security group only accepts
# traffic from the cluster/VPC plus the NodePorts that the public load
# balancer needs (the in-tree cloud provider opens those itself).

variable "name" { type = string }
variable "kubernetes_version" { type = string }
variable "vpc_id" { type = string }
variable "public_subnet_ids" { type = list(string) }
variable "node_instance_type" { type = string }
variable "node_desired_size" { type = number }
variable "extra_admin_arns" { type = list(string) }
variable "app_namespace" { type = string }
variable "app_policy_json" { type = string }

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.31"

  cluster_name    = var.name
  cluster_version = var.kubernetes_version

  cluster_endpoint_public_access           = true
  enable_cluster_creator_admin_permissions = true

  vpc_id     = var.vpc_id
  subnet_ids = var.public_subnet_ids

  cluster_addons = {
    coredns                = { most_recent = true }
    kube-proxy             = { most_recent = true }
    vpc-cni                = { most_recent = true, before_compute = true }
    eks-pod-identity-agent = { most_recent = true, before_compute = true }
  }

  eks_managed_node_groups = {
    default = {
      ami_type       = "AL2023_x86_64_STANDARD"
      instance_types = [var.node_instance_type]
      capacity_type  = "ON_DEMAND"
      min_size       = 2
      max_size       = 4
      desired_size   = var.node_desired_size
      subnet_ids     = var.public_subnet_ids
      disk_size      = 30
      labels         = { workload = "general" }
    }
  }

  # Allow nodes to talk to each other on all ports (Prometheus scraping,
  # gRPC between pods on different nodes, ZooKeeper, etc.).
  node_security_group_additional_rules = {
    ingress_self_all = {
      description = "Node to node all ports"
      protocol    = "-1"
      from_port   = 0
      to_port     = 0
      type        = "ingress"
      self        = true
    }
  }

  access_entries = {
    for idx, arn in var.extra_admin_arns : "admin-${idx}" => {
      principal_arn = arn
      policy_associations = {
        admin = {
          policy_arn   = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
          access_scope = { type = "cluster" }
        }
      }
    }
  }
}

# ---------------------------------------------------------------------------
# IAM for the application pods (EKS Pod Identity). All smart-campus pods use
# the service account "smart-campus-app"; it may use DynamoDB, SQS and S3.
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "pod_identity_trust" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "app" {
  name               = "${var.name}-app-pods"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_trust.json
}

resource "aws_iam_role_policy" "app" {
  name   = "${var.name}-app-access"
  role   = aws_iam_role.app.id
  policy = var.app_policy_json
}

resource "aws_eks_pod_identity_association" "app" {
  cluster_name    = module.eks.cluster_name
  namespace       = var.app_namespace
  service_account = "smart-campus-app"
  role_arn        = aws_iam_role.app.arn
}

output "cluster_name" { value = module.eks.cluster_name }
output "cluster_endpoint" { value = module.eks.cluster_endpoint }
output "cluster_certificate_authority_data" { value = module.eks.cluster_certificate_authority_data }
output "node_security_group_id" { value = module.eks.node_security_group_id }
output "pod_identity_trust_json" { value = data.aws_iam_policy_document.pod_identity_trust.json }
