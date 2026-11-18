# Notification Service = AWS Lambda (requirement b: serverless, async, event-driven).
# Two event sources:
#   1. SQS booking-notifications queue  (booking confirmations / changes / cancellations)
#   2. S3 uploads bucket, prefix imports/ (bulk resource CSV uploaded -> admin notice)
# Packaging follows Practice Sheet 6 Example 1 (archive_file + source_code_hash).

variable "name" { type = string }
variable "source_dir" { type = string }
variable "queue_arn" { type = string }
variable "uploads_bucket" { type = string }
variable "uploads_bucket_arn" { type = string }
variable "notifications_table" { type = string }
variable "notifications_arn" { type = string }
variable "sns_topic_arn" { type = string }

data "archive_file" "zip" {
  type        = "zip"
  source_dir  = var.source_dir
  output_path = "${path.root}/.build/notification-lambda.zip"
  excludes    = ["__pycache__", "tests", "README.md"]
}

data "aws_iam_policy_document" "trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${var.name}-notification-lambda"
  assume_role_policy = data.aws_iam_policy_document.trust.json
}

resource "aws_iam_role_policy_attachment" "logs" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "lambda" {
  statement {
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"]
    resources = [var.queue_arn]
  }
  statement {
    actions   = ["dynamodb:PutItem"]
    resources = [var.notifications_arn]
  }
  statement {
    actions   = ["sns:Publish"]
    resources = [var.sns_topic_arn]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${var.uploads_bucket_arn}/*"]
  }
}

resource "aws_iam_role_policy" "lambda" {
  name   = "${var.name}-notification-lambda"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda.json
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.name}-notification"
  retention_in_days = 7
}

resource "aws_lambda_function" "notification" {
  function_name    = "${var.name}-notification"
  role             = aws_iam_role.lambda.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  timeout          = 30
  memory_size      = 256
  filename         = data.archive_file.zip.output_path
  source_code_hash = data.archive_file.zip.output_base64sha256

  environment {
    variables = {
      NOTIFICATIONS_TABLE = var.notifications_table
      SNS_TOPIC_ARN       = var.sns_topic_arn
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda, aws_iam_role_policy_attachment.logs]
}

# --- trigger 1: SQS
resource "aws_lambda_event_source_mapping" "sqs" {
  event_source_arn        = var.queue_arn
  function_name           = aws_lambda_function.notification.arn
  batch_size              = 10
  function_response_types = ["ReportBatchItemFailures"]
}

# --- trigger 2: S3 object created under imports/
resource "aws_lambda_permission" "s3" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.notification.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = var.uploads_bucket_arn
}

resource "aws_s3_bucket_notification" "uploads" {
  bucket = var.uploads_bucket

  lambda_function {
    lambda_function_arn = aws_lambda_function.notification.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "imports/"
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.s3]
}

output "function_name" { value = aws_lambda_function.notification.function_name }
output "log_group" { value = aws_cloudwatch_log_group.lambda.name }
