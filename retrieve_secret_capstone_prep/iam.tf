variable "keybase_user" {
  description = <<-EOM
    Enter the keybase id of a person to encrypt the AWS IAM secret access key.
    Note that you need access to its private key so you can decrypt it. In
    practice that means you specify your own keybase account id.
    EOM
}

resource "aws_iam_user" "participant" {
  name = "participant"
  path = "/system/"
}

resource "aws_iam_access_key" "participant" {
  user    = aws_iam_user.participant.name
  pgp_key = "keybase:${var.keybase_user}"
}

resource "aws_iam_user_policy" "participant_permissions" {
  name = "read_secret_and_data"
  user = aws_iam_user.participant.name

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetResourcePolicy",
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret",
                "secretsmanager:ListSecretVersionIds",
                "secretsmanager:ListSecrets"
            ],
            "Resource": ["*"]
        },
        {
            "Effect": "Allow",
            "Action": [
              "s3:GetObject"
            ],
            "Resource": ["arn:aws:s3:::dmacademy-course-assets/pyspark/*"]
        },
        {
            "Effect": "Allow",
            "Action": [
              "s3:ListBucket"
            ],
            "Resource": ["arn:aws:s3:::dmacademy-course-assets"]
        }

    ]
}
EOF
}