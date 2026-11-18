# Storage (requirement g): RDS PostgreSQL + DynamoDB + S3, plus the SQS/SNS
# messaging used by the serverless Notification Service (requirement b).

variable "name" { type = string }
variable "vpc_id" { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "eks_node_sg_id" { type = string }
variable "partition_nodes" { type = list(string) }
variable "notification_email" { type = string }

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

resource "random_id" "suffix" {
  byte_length = 3
}

# =========================================================== RDS (SQL)
resource "random_password" "db" {
  length  = 20
  special = false
}

resource "aws_db_subnet_group" "this" {
  name       = "${var.name}-db"
  subnet_ids = var.private_subnet_ids
}

resource "aws_security_group" "db" {
  name        = "${var.name}-db"
  description = "PostgreSQL reachable only from EKS nodes"
  vpc_id      = var.vpc_id
}

# SG-to-SG rule (the Take-Home 2 pattern): only the EKS node SG can reach 5432
resource "aws_security_group_rule" "db_from_eks" {
  type                     = "ingress"
  security_group_id        = aws_security_group.db.id
  source_security_group_id = var.eks_node_sg_id
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  description              = "Postgres from EKS nodes"
}

resource "aws_db_instance" "this" {
  identifier              = "${var.name}-pg"
  engine                  = "postgres"
  engine_version          = "16"
  instance_class          = "db.t4g.micro"
  allocated_storage       = 20
  storage_type            = "gp3"
  username                = "campusadmin"
  password                = random_password.db.result
  db_name                 = "campus"
  db_subnet_group_name    = aws_db_subnet_group.this.name
  vpc_security_group_ids  = [aws_security_group.db.id]
  publicly_accessible     = false
  multi_az                = false
  skip_final_snapshot     = true
  deletion_protection     = false
  backup_retention_period = 0
  apply_immediately       = true
}

# =========================================================== DynamoDB (NoSQL)
# One table per logical storage node on the consistent-hash ring.
# pk = resourceId#date (compound partition key), sk = startTime#bookingId
resource "aws_dynamodb_table" "partition" {
  for_each     = toset(var.partition_nodes)
  name         = "${var.name}-bookings-${each.key}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "S"
  }

  tags = { PartitionNode = each.key }
}

# Flink results, written by the Usage Analytics sink
resource "aws_dynamodb_table" "analytics" {
  name         = "${var.name}-analytics-results"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "resourceId"
  range_key    = "windowStart"

  attribute {
    name = "resourceId"
    type = "S"
  }
  attribute {
    name = "windowStart"
    type = "S"
  }
}

# Notification log written by the Lambda
resource "aws_dynamodb_table" "notifications" {
  name         = "${var.name}-notifications"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "userId"
  range_key    = "sk"

  attribute {
    name = "userId"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "S"
  }
}

# =========================================================== S3 (object store)
resource "aws_s3_bucket" "uploads" {
  bucket        = "${var.name}-uploads-${random_id.suffix.hex}"
  force_destroy = true
}

resource "aws_s3_bucket" "artifacts" {
  bucket        = "${var.name}-artifacts-${random_id.suffix.hex}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "uploads" {
  bucket                  = aws_s3_bucket.uploads.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# =========================================================== SQS + SNS
resource "aws_sqs_queue" "dlq" {
  name                      = "${var.name}-notifications-dlq"
  message_retention_seconds = 1209600
}

resource "aws_sqs_queue" "notifications" {
  name                       = "${var.name}-booking-notifications"
  visibility_timeout_seconds = 90 # > Lambda timeout (30s) x safety factor
  receive_wait_time_seconds  = 10
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_sns_topic" "notifications" {
  name = "${var.name}-booking-notifications"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.notification_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.notifications.arn
  protocol  = "email"
  endpoint  = var.notification_email
}

# =========================================================== policy for app pods
data "aws_iam_policy_document" "app" {
  statement {
    sid = "DynamoDB"
    actions = [
      "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem",
      "dynamodb:Query", "dynamodb:Scan", "dynamodb:BatchWriteItem", "dynamodb:BatchGetItem",
      "dynamodb:DescribeTable",
    ]
    resources = concat(
      [for t in aws_dynamodb_table.partition : t.arn],
      [aws_dynamodb_table.analytics.arn, aws_dynamodb_table.notifications.arn],
    )
  }
  statement {
    sid       = "SQS"
    actions   = ["sqs:SendMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"]
    resources = [aws_sqs_queue.notifications.arn]
  }
  statement {
    sid       = "S3Uploads"
    actions   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.uploads.arn, "${aws_s3_bucket.uploads.arn}/*"]
  }
}

# =========================================================== outputs
output "db_host" { value = aws_db_instance.this.address }
output "db_username" { value = aws_db_instance.this.username }
output "db_password" {
  value     = random_password.db.result
  sensitive = true
}
output "partition_tables" { value = { for k, t in aws_dynamodb_table.partition : k => t.name } }
output "analytics_table" { value = aws_dynamodb_table.analytics.name }
output "notifications_table" { value = aws_dynamodb_table.notifications.name }
output "notifications_table_arn" { value = aws_dynamodb_table.notifications.arn }
output "uploads_bucket" { value = aws_s3_bucket.uploads.bucket }
output "uploads_bucket_arn" { value = aws_s3_bucket.uploads.arn }
output "artifacts_bucket" { value = aws_s3_bucket.artifacts.bucket }
output "artifacts_bucket_arn" { value = aws_s3_bucket.artifacts.arn }
output "notifications_queue_url" { value = aws_sqs_queue.notifications.url }
output "notifications_queue_arn" { value = aws_sqs_queue.notifications.arn }
output "sns_topic_arn" { value = aws_sns_topic.notifications.arn }
output "app_policy_json" { value = data.aws_iam_policy_document.app.json }
