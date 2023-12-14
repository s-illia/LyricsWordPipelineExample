
resource "aws_lambda_function" "func" {
  function_name = "get-convert-raw-data"
  handler       = "download-convert-upload.lambda_handler"
  runtime       = "python3.11"
  timeout       = 300
  memory_size   = 1024

  filename   = "${path.module}/download-convert-upload.zip"

  role  = aws_iam_role.lambda_exec.arn
  layers = [var.aws_wrangler_arn]

  tags = {
    Name = "get-convert-raw-data"
  }
  depends_on = [aws_iam_role_policy_attachment.attach]
}

data "archive_file" "lambda_code" {
type        = "zip"
source_dir  = "${path.module}/../../lambda-download-convert-upload"
output_path = "${path.module}/../../download-convert-upload.zip"
}

resource "aws_iam_role" "lambda_exec" {
  name = "lambda-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com",
        },
      },
    ],
  })

  tags = {
    Name = "lambda-execution-role"
  }
}


resource "aws_iam_policy" "lambda_s3_ssm_access" {
  name        = "LambdaSSMS3AccessPolicy"
  description = "Lambda policy for S3 and SSM access"
  policy      = jsonencode({
                Version: "2012-10-17",
                Statement: [
                    {
                        Sid: "Logs",
                        Effect: "Allow",
                        Action: [
                            "logs:CreateLogStream",
                            "logs:CreateLogGroup",
                            "logs:PutLogEvents"
                        ],
                        Resource: [
                            "arn:aws:logs:*:*:*"
                        ]
                    },
                    {
                        Sid: "s3",
                        Effect: "Allow",
                        Action: [
                            "s3:PutObject",
                            "s3:GetObject"
                        ],
                        Resource: "arn:aws:s3:::*/*"
                    },
                    {
                        Sid: "SSMParams",
                        Effect: "Allow",
                        Action: [
                            "ssm:DescribeParameters",
                            "ssm:GetParameter"
                        ],
                        Resource: "*"
                    }
                ]
            })
}


resource "aws_iam_role_policy_attachment" "attach" {
 role        = aws_iam_role.lambda_exec.name
 policy_arn  = aws_iam_policy.lambda_s3_ssm_access.arn
}

resource "aws_ssm_parameter" "lambda_config" {
  name  = "${var.ssm_prefix}/lambda-config"
  type  = "String"
  value = jsonencode(
      {
        "sources":[
            {
              "name":"genres.tsv",
              "url":"https://www.tagtraum.com/genres/msd_tagtraum_cd2c.cls.zip",
              "format":"csv",
              "sep":"\t",
              "skiprows":7,
              "header":"None",
              "column_names":[
                  "track_id",
                  "genre"
              ]
            },
            {
              "name":"unstemmed_mapping.txt",
              "url":"http://millionsongdataset.com/sites/default/files/mxm_reverse_mapping.txt",
              "format":"csv",
              "sep":"<SEP>",
              "skiprows":0,
              "header":"None",
              "column_names":[
                  "stemmed",
                  "unstemmed"
              ]
            },
            {
              "name":"id-track-artist.txt",
              "url":"http://millionsongdataset.com/sites/default/files/AdditionalFiles/mxm_779k_matches.txt.zip",
              "format":"csv",
              "sep":"<SEP>",
              "skiprows":18,
              "header":"None",
              "column_names":[
                  "msd_track_id",
                  "msd_artist_name",
                  "msd_title",
                  "mxm_track_id",
                  "mxm_artist_name",
                  "mxm_title"
              ]
            }
        ],
        "download_path":"/tmp"
      }
  )
  }
