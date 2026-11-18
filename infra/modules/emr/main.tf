# Flink on EMR (requirement e):
#  - runs in our own VPC/public subnet (not the default VPC)
#  - creates the EMR service role + EC2 instance profile here, instead of
#    `aws emr create-default-roles` (so requirement a holds: no manual IAM)
#  - our own EMR-managed security groups (revoke_rules_on_delete lets
#    `terraform destroy` remove them cleanly)
#  - a bootstrap action installs the CloudWatch agent so Flink JobManager /
#    TaskManager logs reach CloudWatch Logs -> Grafana (requirement h)
#  - no task group and no SSH key: the job is submitted as an EMR step
#    (scripts/flink-submit.sh), so nobody needs to SSH in

variable "name" { type = string }
variable "vpc_id" { type = string }
variable "vpc_cidr" { type = string }
variable "subnet_id" { type = string }
variable "release_label" { type = string }
variable "instance_type" { type = string }
variable "artifacts_bucket" { type = string }
variable "flink_log_group" { type = string }

data "aws_partition" "current" {}

# ---------------------------------------------------------- IAM
data "aws_iam_policy_document" "emr_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["elasticmapreduce.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "service" {
  name               = "${var.name}-emr-service"
  assume_role_policy = data.aws_iam_policy_document.emr_trust.json
}

# Same managed policy that `aws emr create-default-roles` attaches to EMR_DefaultRole
resource "aws_iam_role_policy_attachment" "service" {
  role       = aws_iam_role.service.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

data "aws_iam_policy_document" "ec2_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ec2" {
  name               = "${var.name}-emr-ec2"
  assume_role_policy = data.aws_iam_policy_document.ec2_trust.json
}

# Same managed policy as EMR_EC2_DefaultRole
resource "aws_iam_role_policy_attachment" "ec2_emr" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

resource "aws_iam_role_policy_attachment" "ec2_cw_agent" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/CloudWatchAgentServerPolicy"
}

resource "aws_iam_role_policy_attachment" "ec2_ssm" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "ec2" {
  name = "${var.name}-emr-ec2"
  role = aws_iam_role.ec2.name
}

# ---------------------------------------------------------- security groups
resource "aws_security_group" "master" {
  name                   = "${var.name}-emr-master"
  description            = "EMR master (rules managed by EMR)"
  vpc_id                 = var.vpc_id
  revoke_rules_on_delete = true

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    ignore_changes = [ingress, egress]
  }
}

resource "aws_security_group" "core" {
  name                   = "${var.name}-emr-core"
  description            = "EMR core (rules managed by EMR)"
  vpc_id                 = var.vpc_id
  revoke_rules_on_delete = true

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    ignore_changes = [ingress, egress]
  }
}

# ---------------------------------------------------------- bootstrap action
resource "aws_s3_object" "cw_bootstrap" {
  bucket = var.artifacts_bucket
  key    = "emr/bootstrap-cloudwatch-agent.sh"
  content = templatefile("${path.module}/bootstrap-cloudwatch-agent.sh.tpl", {
    log_group = var.flink_log_group
  })
  content_type = "text/x-shellscript"
}

# ---------------------------------------------------------- cluster
resource "aws_emr_cluster" "this" {
  name          = "${var.name}-flink"
  release_label = var.release_label
  applications  = ["Hadoop", "Flink"]
  service_role  = aws_iam_role.service.arn
  log_uri       = "s3://${var.artifacts_bucket}/emr-logs/"

  keep_job_flow_alive_when_no_steps = true
  termination_protection            = false

  ec2_attributes {
    subnet_id                         = var.subnet_id
    instance_profile                  = aws_iam_instance_profile.ec2.arn
    emr_managed_master_security_group = aws_security_group.master.id
    emr_managed_slave_security_group  = aws_security_group.core.id
  }

  master_instance_group {
    instance_type  = var.instance_type
    instance_count = 1
  }

  core_instance_group {
    instance_type  = var.instance_type
    instance_count = 1
  }

  bootstrap_action {
    name = "install-cloudwatch-agent"
    path = "s3://${var.artifacts_bucket}/${aws_s3_object.cw_bootstrap.key}"
  }

  configurations_json = jsonencode([
    {
      Classification = "flink-conf"
      Properties = {
        "taskmanager.numberOfTaskSlots" = "2"
        "parallelism.default"           = "2"
      }
    }
  ])

  tags = {
    Purpose = "Cloud Computing"
    Lab     = "Assignment-S4"
  }

  depends_on = [
    aws_iam_role_policy_attachment.service,
    aws_iam_role_policy_attachment.ec2_emr,
  ]
}

output "cluster_id" { value = aws_emr_cluster.this.id }
output "master_public_dns" { value = aws_emr_cluster.this.master_public_dns }
