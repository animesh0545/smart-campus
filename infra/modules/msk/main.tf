# Amazon MSK provisioned: 2 x kafka.t3.small, plaintext inside the VPC.
# Takes 20-30 minutes to create, so scripts/bootstrap.sh starts it first.

variable "name" { type = string }
variable "vpc_id" { type = string }
variable "vpc_cidr" { type = string }
variable "private_subnet_ids" { type = list(string) }

variable "kafka_version" {
  type    = string
  default = "3.9.x"
}

resource "aws_security_group" "msk" {
  name        = "${var.name}-msk"
  description = "Kafka plaintext from inside the VPC (EKS pods and EMR)"
  vpc_id      = var.vpc_id

  ingress {
    description = "Kafka plaintext"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_msk_configuration" "this" {
  name           = "${var.name}-kafka-config"
  kafka_versions = [var.kafka_version]

  server_properties = <<-PROPS
    auto.create.topics.enable=true
    default.replication.factor=2
    min.insync.replicas=1
    num.partitions=3
    log.retention.hours=48
  PROPS
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/msk/${var.name}"
  retention_in_days = 7
}

resource "aws_msk_cluster" "this" {
  cluster_name           = "${var.name}-kafka"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = 2

  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    client_subnets  = var.private_subnet_ids
    security_groups = [aws_security_group.msk.id]
    storage_info {
      ebs_storage_info {
        volume_size = 10
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "PLAINTEXT"
      in_cluster    = true
    }
  }

  client_authentication {
    unauthenticated = true
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }
}

output "bootstrap_brokers" { value = aws_msk_cluster.this.bootstrap_brokers }
output "cluster_arn" { value = aws_msk_cluster.this.arn }
