resource "aws_s3_bucket" "b" {
  bucket = "lyrics-word-raw-bucket"
  #bucket_prefix = 
  force_destroy = true
  acl    = "private"
  tags = {
    Name = "LyricsWordRawBucket"
  }
}

resource "aws_ssm_parameter" "bucket_name" {
  name  = "${var.ssm_prefix}/${var.s3_bucket_logical_name}/name"
  type  = "String"
  value = aws_s3_bucket.b.id
}

resource "aws_ssm_parameter" "bucket_arn" {
  name  = "${var.ssm_prefix}/${var.s3_bucket_logical_name}/arn"
  type  = "String"
  value = aws_s3_bucket.b.arn
}