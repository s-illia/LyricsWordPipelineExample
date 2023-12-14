resource "aws_scheduler_schedule" "trigger_update" {
  name                = var.eventbridge_schedule_name
  group_name = "default"
  flexible_time_window {
    mode = "OFF"
  }
  schedule_expression = "cron(1 12 * * ? *)"
  state          = "DISABLED"
  target {
    arn      = var.target_lambda_arn
    role_arn = aws_iam_role.scheduler_execution_role.arn
  }
}

resource "aws_iam_role" "scheduler_execution_role" {
  name = "scheduler-execution-role"

  assume_role_policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "scheduler.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
})

  tags = {
    Name = "scheduler-execution-role"
  }
}


resource "aws_iam_policy" "scheduler_lambda_invoke" {
  name        = "scheduler_lambda_invoke"
  policy      = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "lambda:InvokeFunction"
            ],
            "Effect": "Allow",
            "Resource": "*"
        }
    ]
})
}

resource "aws_iam_role_policy_attachment" "attach" {
 role        = aws_iam_role.scheduler_execution_role.name
 policy_arn  = aws_iam_policy.scheduler_lambda_invoke.arn
}
