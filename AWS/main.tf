provider "aws" {
  region = var.aws_region
}

module "s3_bucket" {
  source      = "./modules/s3_bucket"
  s3_bucket_logical_name = var.s3_bucket_logical_name
  ssm_prefix = var.ssm_prefix
}

module "lambda_function" {
  source       = "./modules/lambda_function"
  aws_region   = var.aws_region
  aws_wrangler_arn = var.aws_wrangler_arn
  depends_on = [module.s3_bucket]
  ssm_prefix = var.ssm_prefix
}

module "eventbridge_schedule" {
  source             = "./modules/eventbridge_schedule"
  depends_on = [module.lambda_function]
  target_lambda_arn = module.lambda_function.func_arn
  eventbridge_schedule_name = var.eventbridge_schedule_name
}
