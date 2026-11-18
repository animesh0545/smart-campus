# GitOps bootstrap (requirement d). Terraform does only the cluster-level
# wiring that has to exist before Git can take over:
#   1. the smart-campus namespace + the ConfigMap/Secret holding the
#      endpoints of the Terraform-created AWS resources (RDS, MSK, SQS, ...)
#   2. ArgoCD itself (Helm)
#   3. ONE root Application (app-of-apps) pointing at gitops/root in the repo
# Every service, HPA, gateway, ZooKeeper and the whole monitoring stack is then
# deployed by ArgoCD from Git. Nobody runs `kubectl apply` for services.

variable "app_namespace" { type = string }
variable "git_repo_url" { type = string }
variable "git_revision" { type = string }
variable "aws_region" { type = string }
variable "platform_config" { type = map(string) }
variable "platform_secrets" {
  type      = map(string)
  sensitive = true
}

resource "kubernetes_namespace" "app" {
  metadata {
    name = var.app_namespace
    labels = {
      "app.kubernetes.io/part-of" = "smart-campus"
    }
  }
}

resource "kubernetes_namespace" "monitoring" {
  metadata {
    name = "monitoring"
  }
}

resource "kubernetes_config_map" "platform" {
  metadata {
    name      = "platform-config"
    namespace = kubernetes_namespace.app.metadata[0].name
  }
  data = var.platform_config
}

resource "kubernetes_secret" "platform" {
  metadata {
    name      = "platform-secrets"
    namespace = kubernetes_namespace.app.metadata[0].name
  }
  data = var.platform_secrets
}

resource "helm_release" "argocd" {
  name             = "argocd"
  namespace        = "argocd"
  create_namespace = true
  repository       = "https://argoproj.github.io/argo-helm"
  chart            = "argo-cd"
  version          = "7.7.11"
  timeout          = 900

  values = [yamlencode({
    configs = {
      params = {
        "server.insecure" = true # plain HTTP behind kubectl port-forward
      }
      cm = {
        "timeout.reconciliation" = "60s"
      }
    }
    dex           = { enabled = false }
    notifications = { enabled = false }
  })]
}

# Root app-of-apps
resource "helm_release" "root_app" {
  name       = "argocd-root-app"
  namespace  = "argocd"
  repository = "https://argoproj.github.io/argo-helm"
  chart      = "argocd-apps"
  version    = "2.0.2"

  values = [yamlencode({
    applications = {
      smart-campus-root = {
        namespace  = "argocd"
        project    = "default"
        finalizers = ["resources-finalizer.argocd.argoproj.io"]
        source = {
          repoURL        = var.git_repo_url
          targetRevision = var.git_revision
          path           = "gitops/root"
          # gitops/root is a small Helm chart: the repo URL is injected here once
          helm = {
            valuesObject = {
              repoURL        = var.git_repo_url
              targetRevision = var.git_revision
              awsRegion      = var.aws_region
            }
          }
        }
        destination = {
          server    = "https://kubernetes.default.svc"
          namespace = "argocd"
        }
        syncPolicy = {
          automated = { prune = true, selfHeal = true }
        }
      }
    }
  })]

  depends_on = [helm_release.argocd, kubernetes_config_map.platform, kubernetes_secret.platform]
}
