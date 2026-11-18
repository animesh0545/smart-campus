# AWS side of observability (requirement h) + cost guard:
#  - CloudWatch log group that the EMR nodes ship Flink JobManager/TaskManager
#    logs to (Lambda's own log group is created in the lambda module)
#  - IAM role that lets Grafana (in EKS) read CloudWatch Logs/Metrics, so Flink
#    and Lambda logs appear in the same Grafana as the Loki pod logs
#  - AWS Budgets alert

variable "name" { type = string }
variable "cluster_name" { type = string }
variable "budget_email" { type = string }
variable "budget_limit_usd" { type = number }

resource "aws_cloudwatch_log_group" "flink" {
  name              = "/${var.name}/flink"
  retention_in_days = 7
}

data "aws_iam_policy_document" "pod_identity_trust" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "grafana" {
  name               = "${var.name}-grafana-cloudwatch"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_trust.json
}

# Same permissions as Grafana's documented CloudWatch data source policy (read only)
data "aws_iam_policy_document" "grafana" {
  statement {
    actions = [
      "cloudwatch:DescribeAlarmsForMetric", "cloudwatch:DescribeAlarmHistory", "cloudwatch:DescribeAlarms",
      "cloudwatch:ListMetrics", "cloudwatch:GetMetricData", "cloudwatch:GetInsightRuleReport",
      "logs:DescribeLogGroups", "logs:GetLogGroupFields", "logs:StartQuery", "logs:StopQuery",
      "logs:GetQueryResults", "logs:GetLogEvents", "logs:FilterLogEvents", "logs:DescribeLogStreams",
      "ec2:DescribeTags", "ec2:DescribeInstances", "ec2:DescribeRegions", "tag:GetResources",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "grafana" {
  name   = "${var.name}-grafana-cloudwatch"
  role   = aws_iam_role.grafana.id
  policy = data.aws_iam_policy_document.grafana.json
}

# The kube-prometheus-stack release in gitops/ is named "kps", so Grafana's SA is "kps-grafana"
resource "aws_eks_pod_identity_association" "grafana" {
  cluster_name    = var.cluster_name
  namespace       = "monitoring"
  service_account = "kps-grafana"
  role_arn        = aws_iam_role.grafana.arn
}

resource "aws_budgets_budget" "monthly" {
  name         = "${var.name}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_email]
  }
}

output "flink_log_group" { value = aws_cloudwatch_log_group.flink.name }
output "flink_log_group_arn" { value = aws_cloudwatch_log_group.flink.arn }
output "grafana_role_arn" { value = aws_iam_role.grafana.arn }
